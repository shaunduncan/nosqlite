import cPickle as pickle
import sqlite3
import sys

from functools import partial
from itertools import ifilter, imap, starmap

"""
LOGICAL
$or : {"$or": [{"a": 1}, {"b": {"$gt": 5}}]}
$and: {"$and": [{"a": 1}, {"b": {"$gt": 5}}]}
$not: {"$not": {"a": {"$gt": 5}}}  # {"a": {"$lte": 5}} should return ONLY if field exists
$nor: {"$nor": [{"a": 1}, {"b": 5}]}  # a != 1 AND b != 5

ELEMENT
$exists: {"a": {"$exists": true, "$nin": [1, 2, 3]}}  # enforce field existence check
$mod   : {"a": {"$mod": [4, 0]}}  # a % 4 == 0
"""


class MalformedQueryException(Exception):
    pass


class Connection(object):
    """
    The high-level connection to a sqlite database. Creating a connection accepts
    the same args and keyword args as the ``sqlite3.connect`` method
    """

    def __init__(self, *args, **kwargs):
        self._collections = {}
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        """
        Connect to a sqlite database only if no connection exists. Isolation level
        for the connection is automatically set to autocommit
        """
        self.db = sqlite3.connect(*args, **kwargs)
        self.db.isolation_level = None

    def close(self):
        """
        Terminate the connection to the sqlite database
        """
        if self.db is not None:
            self.db.close()

    def __getitem__(self, name):
        """
        A pymongo-like behavior for dynamically obtaining a collection of documents
        """
        if name not in self._collections:
            self._collections[name] = Collection(self.db, name)
        return self._collections[name]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.close()
        return False

    def drop_collection(self, name):
        """
        Drops a collection permanently if it exists
        """
        self.db.execute("drop table if exists %s" % name)


class Collection(object):
    """
    A virtual database table that holds JSON-type documents
    """

    def __init__(self, db, name, create=True):
        self.db = db
        self.name = name

        if create:
            self.create()

    def exists(self):
        """
        Checks if this collection exists
        """
        row = self.db.execute("""
            select count(1) from sqlite_master where type = 'table' and name = ?
        """, (self.name,)).fetchone()

        return int(row[0]) > 0

    def create(self):
        """
        Creates the collections database only if it does not already exist
        """
        self.db.execute("""
            create table if not exists %s (
                id integer primary key autoincrement,
                data blob not null
            )
        """ % self.name)

    def insert(self, document):
        """
        Inserts a document into this collection. If a document already has an '_id'
        value it will be updated

        :returns: inserted document with id
        """
        if '_id' in document:
            return self.update(document)

        # Create it and return a modified one with the id
        cursor = self.db.execute("""
            insert into %s(data) values (?)
        """ % self.name, (pickle.dumps(document),))

        document['_id'] = cursor.lastrowid
        return document

    def update(self, document):
        """
        Updates a document stored in this collection. If the document does not
        already have an '_id' value, it will be created
        """
        if '_id' not in document:
            return self.insert(document)

        # Update the stored document, removing the id
        copy = document.copy()
        del copy['_id']

        self.db.execute("""
            update %s set data = ? where id = ?
        """ % self.name, (pickle.dumps(copy), document['_id']))

        return document

    def remove(self, document):
        """
        Removes a document from this collection. This will raise AssertionError if the
        document does not have an _id attribute
        """
        assert '_id' in document, 'Document must have an id'
        self.db.execute("delete from %s where id = ?" % self.name, (document['_id'],))

    def save(self, document):
        """
        Alias for ``update``
        """
        return self.update(document)

    def delete(self, document):
        """
        Alias for ``remove``
        """
        return self.remove(document)

    def _unpickle(self, id, data):
        """
        Loads a pickled document taking care to apply the document id
        """
        document = pickle.loads(data.encode('utf-8'))
        document['_id'] = id
        return document

    def _has_all_keys(self, keys, document):
        """
        Returns True if a document (dict) has every key in list of keys

        :param keys: an iterable (list, tuple) of keys to check
        :param document: a python dict
        """
        keyset = set(keys)
        return keyset.intersection(document.keys()) == keyset

    def _has_any_key(self, keys, document):
        """
        Returns true if a document (dict) has any of list of keys

        :param keys: an iterable (list, tuple) of keys to check
        :param document: a python dict
        """
        keyset = set(keys)
        return len(keyset.intersection(document.keys())) > 0

    def find(self, query=None, limit=None):
        """
        Returns a list of documents in this collection that match a given query
        """
        results = []
        query = query or {}

        # TODO: When indexes are implemented, we'll need to intelligently hit one of the
        # index stores so we don't do a full table scan
        cursor = self.db.execute("select id, data from %s" % self.name)
        apply = partial(self._apply_query, query)

        for match in ifilter(apply, starmap(self._unpickle, cursor.fetchall())):
            results.append(match)

            # Just return if we already reached the limit
            if limit and len(results) == limit:
                return results

        return results

    def _apply_query(self, query, document):
        """
        Applies a query to a document. Returns True if the document meets the criteria of
        the supplied query
        """
        matches = []  # A list of booleans
        reapply = lambda q: self._apply_query(q, document)

        # FIXME: Fields need to support dot notation for sub-documents
        # i.e. {'foo.bar': 5} --> doc['foo']['bar'] == 5
        for field, value in query.iteritems():
            # A more complex query type $and, $or, etc
            if field == '$and':
                matches.append(all(imap(reapply, value)))
            elif field == '$or':
                matches.append(any(imap(reapply, value)))
            elif field == '$nor':
                matches.append(not any(imap(reapply, value)))
            elif field == '$not':
                matches.append(not self._apply_query(value, document))

            # Invoke a query operator
            elif isinstance(value, dict):
                for operator, arg in value.iteritems():
                    if not self._get_operator_fn(operator)(field, arg, document):
                        matches.append(False)
                        break
                else:
                    matches.append(True)

            # Standard
            elif value != document.get(field, None):
                matches.append(False)

        return all(matches)

    def _get_operator_fn(self, op):
        """
        Returns True if operator such as $gt or $eq is a valid operator.
        This simly checks if there is a method that handles the operator defined
        in this module, replacing '$' with '_' (i.e. if this module has a _gt
        method for $gt)
        """
        if not op.startswith('$'):
            raise MalformedQueryException("Operator '%s' is not a valid query operation" % op)

        try:
            return getattr(sys.modules[__name__], op.replace('$', '_'))
        except AttributeError:
            raise MalformedQueryException("Operator '%s' is not currently implemented" % op)

    def find_one(self, query=None):
        """
        Equivalent to ``find(query, limit=1)[0]``
        """
        return self.find(query=query, limit=1)[0]

    def find_and_modify(self, query=None, update=None):
        """
        Finds documents in this collection that match a given query and updates them
        """
        update = update or {}

        for document in self.find(query=query):
            document.update(update)
            self.update(document)

    def count(self, query=None):
        """
        Equivalent to ``len(find(query))``
        """
        return len(self.find(query=query))

    def create_index(self):
        pass

    def ensure_index(self):
        pass

    def drop_index(self):
        pass

    def drop_indexes(self):
        """
        Drop all indexes for this collection
        """
        pass

    def rename(self, new_name):
        pass

    def distinct(self, key):
        pass


# BELOW ARE OPERATIONS FOR LOOKUPS
def _eq(field, value, document):
    """
    Returns True if the value of a document field is equal to a given value
    """
    return document.get(field, None) == value


def _gt(field, value, document):
    """
    Returns True if the value of a document field is greater than a given value
    """
    return document.get(field, None) > value


def _lt(field, value, document):
    """
    Returns True if the value of a document field is less than a given value
    """
    return document.get(field, None) < value


def _gte(field, value, document):
    """
    Returns True if the value of a document field is greater than or
    equal to a given value
    """
    return document.get(field, None) >= value


def _lte(field, value, document):
    """
    Returns True if the value of a document field is less than or
    equal to a given value
    """
    return document.get(field, None) <= value


def _all(field, value, document):
    """
    Returns True if the value of document field contains all the values
    specified by ``value``. If supplied value is not an iterable, a
    MalformedQueryException is raised. If the value of the document field
    is not an iterable, False is returned
    """
    try:
        a = set(value)
    except TypeError:
        raise MalformedQueryException("'$all' must accept an iterable")

    try:
        b = set(document.get(field, []))
    except TypeError:
        return False
    else:
        return a.intersection(b) == a


def _in(field, value, document):
    """
    Returns True if document[field] is in the interable value. If the
    supplied value is not an iterable, then a MalformedQueryException is raised
    """
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$in' must accept an iterable")

    return document.get(field, None) in values


def _ne(field, value, document):
    """
    Returns True if the value of document[field] is not equal to a given value
    """
    return document.get(field, None) != value


def _nin(field, value, document):
    """
    Returns True if document[field] is NOT in the interable value. If the
    supplied value is not an iterable, then a MalformedQueryException is raised
    """
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$nin' must accept an iterable")

    return document.get(field, None) not in values


def _mod(field, value, document):
    """
    Performs a mod on a document field. Value must be a list or tuple with
    two values divisor and remainder (i.e. [2, 0]). This will essentially
    perform the following:

        document[field] % divisor == remainder

    If the value does not contain integers or is not a two-item list/tuple,
    a MalformedQueryException will be raised. If the value of document[field]
    cannot be converted to an integer, this will return False.
    """
    try:
        divisor, remainder = map(int, value)
    except (TypeError, ValueError):
        raise MalformedQueryException("'$mod' must accept an iterable: [divisor, remainder]")

    try:
        return int(document.get(field, None)) % divisor == remainder
    except (TypeError, ValueError):
        return False
