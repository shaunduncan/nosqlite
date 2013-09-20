import cPickle as pickle
import sqlite3

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

    def _load_document(self, id, data):
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
        fn = partial(self._apply_query, query)

        for doc in ifilter(fn, starmap(self._load_document, cursor.fetchall())):
            results.append(doc)

            # Just return if we already reached the limit
            if limit and len(results) == limit:
                return results

        return results

    def _apply_query(self, query, document):
        matches = []  # A list of booleans
        reapply = lambda q: self._apply_query(q, document)

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
                for operation, arg in value.iteritems():
                    # FIXME
                    pass

            # Standard
            elif value != document.get(field, None):
                matches.append(False)

        return all(matches)

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
    return document.get(field, None) == value


def _gt(field, value, document):
    return document.get(field, None) > value


def _lt(field, value, document):
    return document.get(field, None) < value


def _gte(field, value, document):
    return document.get(field, None) >= value


def _lte(field, value, document):
    return document.get(field, None) <= value


def _all(field, value, document):
    a = set(value)
    b = set(document.get(field, []))
    return a.intersection(b) == a


def _in(field, value, document):
    return document.get(field, None) in value


def _ne(field, value, document):
    return document.get(field, None) != value


def _nin(field, value, document):
    return document.get(field, None) not in value
