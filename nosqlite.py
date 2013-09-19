import cPickle as pickle
import sqlite3

"""
OPERATIONS
$all: {"a": {"$all": [1, 2, 3]}}  # == all()
$gt : {"a": {"$gt": 5}}
$gte: {"a": {"$gte": 5}}
$in : {"a": {"$in": [1, 2, 3]}}  # == any()
$lt : {"a": {"$lt": 5}}
$lte: {"a": {"$lte": 5}}
$ne : {"a": {"$ne": 5}}  # not equal
$nin: {"a": {"$nin": [1, 2, 3]}}  # not in

LOGICAL
$or : {"$or": [{"a": 1}, {"b": {"$gt": 5}}]}
$and: {"$and": [{"a": 1}, {"b": {"$gt": 5}}]}
$not: {"$not": {"a": {"$gt": 5}}}  # {"a": {"$lte": 5}} should return ONLY if field exists
$nor: {"$nor": [{"a": 1}, {"b": 5}]}  # a != 1 AND b != 5

ELEMENT
$exists: {"a": {"$exists": true, "$nin": [1, 2, 3]}}  # enforce field existence check
$mod   : {"a": {"$mod": [4, 0]}}  # a % 4 == 0

UPDATES
$set  : coll.update(...filter..., {"$set": {"a": 5}})
$unset: coll.update(...filter..., {"$unset": {"a": ""}})  # Remove field

...
http://docs.mongodb.org/manual/reference/operator/
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

    def find(self):
        pass

    def find_one(self):
        pass

    def find_and_modify(self):
        pass

    def count(self):
        pass

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
