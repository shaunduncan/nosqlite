import re
import sqlite3

from unittest import TestCase

from mock import Mock, patch
from pytest import raises

import nosqlite


class ConnectionTestCase(TestCase):

    def test_connect(self):
        conn = nosqlite.Connection(':memory:')
        assert conn.db.isolation_level is None

    @patch('nosqlite.sqlite3')
    def test_context_manager_closes_connection(self, sqlite):
        with nosqlite.Connection() as conn:
            pass

        assert conn.db.close.called

    @patch('nosqlite.sqlite3')
    @patch('nosqlite.Collection')
    def test_getitem_returns_collection(self, mock_collection, sqlite):
        sqlite.connect.return_value = sqlite
        mock_collection.return_value = mock_collection
        conn = nosqlite.Connection()

        assert 'foo' not in conn._collections
        assert conn['foo'] == mock_collection

    @patch('nosqlite.sqlite3')
    def test_getitem_returns_cached_collection(self, sqlite):
        conn = nosqlite.Connection()
        conn._collections['foo'] = 'bar'

        assert conn['foo'] == 'bar'

    @patch('nosqlite.sqlite3')
    def test_drop_collection(self, sqlite):
        conn = nosqlite.Connection()
        conn.drop_collection('foo')

        assert "drop table if exists foo" == conn.db.execute.call_args_list[0][0][0]


class CollectionTestCase(TestCase):

    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.collection = nosqlite.Collection(self.db, 'foo', create=False)

    def tearDown(self):
        self.db.close()

    def unformat_sql(self, sql):
        return re.sub(r'[\s]+', ' ', sql.strip().replace('\n', ''))

    def test_create_has_correct_sql(self):
        collection = nosqlite.Collection(Mock(), 'foo', create=False)
        collection.create()
        assert "create table if not exists foo" in collection.db.execute.call_args_list[0][0][0]

    def test_exists_when_absent(self):
        assert not self.collection.exists()

    def test_exists_when_present(self):
        self.collection.create()
        assert self.collection.exists()

    def test_insert_actually_updates(self):
        doc = {'_id': 1, 'foo': 'bar'}

        self.collection.update = Mock()
        self.collection.insert(doc)
        self.collection.update.assert_called_with(doc)

    def test_insert(self):
        doc = {'foo': 'bar'}

        self.collection.create()
        inserted = self.collection.insert(doc)
        assert inserted['_id'] == 1

    def test_update_actually_inserts(self):
        doc = {'foo': 'bar'}

        self.collection.insert = Mock()
        self.collection.update(doc)
        self.collection.insert.assert_called_with(doc)

    def test_update(self):
        doc = {'foo': 'bar'}

        self.collection.create()
        doc = self.collection.insert(doc)
        doc['foo'] = 'baz'

        updated = self.collection.update(doc)
        assert updated['foo'] == 'baz'

    def test_remove_raises_when_no_id(self):
        with raises(AssertionError):
            self.collection.remove({'foo': 'bar'})

    def test_remove(self):
        self.collection.create()
        doc = self.collection.insert({'foo': 'bar'})
        assert 1 == int(self.collection.db.execute("select count(1) from foo").fetchone()[0])

        self.collection.remove(doc)
        assert 0 == int(self.collection.db.execute("select count(1) from foo").fetchone()[0])

    def test_find(self):
        query = {'foo': 'bar'}
        documents = [
            (1, {'foo': 'bar', 'baz': 'qux'}),  # Will match
            (2, {'foo': 'bar', 'bar': 'baz'}),  # Will match
            (2, {'foo': 'baz', 'bar': 'baz'}),  # Will not match
            (3, {'baz': 'qux'}),  # Will not match
        ]

        collection = nosqlite.Collection(Mock(), 'foo', create=False)
        collection.db.execute.return_value = collection.db
        collection.db.fetchall.return_value = documents
        collection._load = lambda id, data: data

        ret = collection.find(query)
        assert len(ret) == 2

    def test_find_honors_limit(self):
        query = {'foo': 'bar'}
        documents = [
            (1, {'foo': 'bar', 'baz': 'qux'}),  # Will match
            (2, {'foo': 'bar', 'bar': 'baz'}),  # Will match
            (2, {'foo': 'baz', 'bar': 'baz'}),  # Will not match
            (3, {'baz': 'qux'}),  # Will not match
        ]

        collection = nosqlite.Collection(Mock(), 'foo', create=False)
        collection.db.execute.return_value = collection.db
        collection.db.fetchall.return_value = documents
        collection._load = lambda id, data: data

        ret = collection.find(query, limit=1)
        assert len(ret) == 1

    def test_apply_query_and_type(self):
        query = {'$and': [{'foo': 'bar'}, {'baz': 'qux'}]}

        assert self.collection._apply_query(query, {'foo': 'bar', 'baz': 'qux'})
        assert not self.collection._apply_query(query, {'foo': 'bar', 'baz': 'foo'})

    def test_apply_query_or_type(self):
        query = {'$or': [{'foo': 'bar'}, {'baz': 'qux'}]}

        assert self.collection._apply_query(query, {'foo': 'bar', 'abc': 'xyz'})
        assert self.collection._apply_query(query, {'baz': 'qux', 'abc': 'xyz'})
        assert not self.collection._apply_query(query, {'abc': 'xyz'})

    def test_apply_query_not_type(self):
        query = {'$not': {'foo': 'bar'}}

        assert self.collection._apply_query(query, {'foo': 'baz'})
        assert not self.collection._apply_query(query, {'foo': 'bar'})

    def test_apply_query_nor_type(self):
        query = {'$nor': [{'foo': 'bar'}, {'baz': 'qux'}]}

        assert self.collection._apply_query(query, {'foo': 'baz', 'baz': 'bar'})
        assert not self.collection._apply_query(query, {'foo': 'bar'})
        assert not self.collection._apply_query(query, {'baz': 'qux'})
        assert not self.collection._apply_query(query, {'foo': 'bar', 'baz': 'qux'})

    def test_apply_query_gt_operator(self):
        query = {'foo': {'$gt': 5}}

        assert self.collection._apply_query(query, {'foo': 10})
        assert not self.collection._apply_query(query, {'foo': 4})

    def test_apply_query_gte_operator(self):
        query = {'foo': {'$gte': 5}}

        assert self.collection._apply_query(query, {'foo': 5})
        assert not self.collection._apply_query(query, {'foo': 4})

    def test_apply_query_lt_operator(self):
        query = {'foo': {'$lt': 5}}

        assert self.collection._apply_query(query, {'foo': 4})
        assert not self.collection._apply_query(query, {'foo': 10})

    def test_apply_query_lte_operator(self):
        query = {'foo': {'$lte': 5}}

        assert self.collection._apply_query(query, {'foo': 5})
        assert not self.collection._apply_query(query, {'foo': 10})

    def test_apply_query_eq_operator(self):
        query = {'foo': {'$eq': 5}}

        assert self.collection._apply_query(query, {'foo': 5})
        assert not self.collection._apply_query(query, {'foo': 4})
        assert not self.collection._apply_query(query, {'foo': 'bar'})

    def test_apply_query_in_operator(self):
        query = {'foo': {'$in': [1, 2, 3]}}

        assert self.collection._apply_query(query, {'foo': 1})
        assert not self.collection._apply_query(query, {'foo': 4})
        assert not self.collection._apply_query(query, {'foo': 'bar'})

    def test_apply_query_in_operator_raises(self):
        query = {'foo': {'$in': 5}}

        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {'foo': 1})

    def test_apply_query_nin_operator(self):
        query = {'foo': {'$nin': [1, 2, 3]}}

        assert self.collection._apply_query(query, {'foo': 4})
        assert self.collection._apply_query(query, {'foo': 'bar'})
        assert not self.collection._apply_query(query, {'foo': 1})

    def test_apply_query_nin_operator_raises(self):
        query = {'foo': {'$nin': 5}}

        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {'foo': 1})

    def test_apply_query_ne_operator(self):
        query = {'foo': {'$ne': 5}}

        assert self.collection._apply_query(query, {'foo': 1})
        assert self.collection._apply_query(query, {'foo': 'bar'})
        assert not self.collection._apply_query(query, {'foo': 5})

    def test_apply_query_all_operator(self):
        query = {'foo': {'$all': [1, 2, 3]}}

        assert self.collection._apply_query(query, {'foo': range(10)})
        assert not self.collection._apply_query(query, {'foo': ['bar', 'baz']})
        assert not self.collection._apply_query(query, {'foo': 3})

    def test_apply_query_all_operator_raises(self):
        query = {'foo': {'$all': 3}}

        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {'foo': 'bar'})

    def test_apply_query_mod_operator(self):
        query = {'foo': {'$mod': [2, 0]}}

        assert self.collection._apply_query(query, {'foo': 4})
        assert not self.collection._apply_query(query, {'foo': 3})
        assert not self.collection._apply_query(query, {'foo': 'bar'})

    def test_apply_query_mod_operator_raises(self):
        query = {'foo': {'$mod': 2}}

        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {'foo': 5})

    def test_apply_query_honors_multiple_operators(self):
        query = {'foo': {'$gte': 0, '$lte': 10, '$mod': [2, 0]}}

        assert self.collection._apply_query(query, {'foo': 4})
        assert not self.collection._apply_query(query, {'foo': 3})
        assert not self.collection._apply_query(query, {'foo': 15})
        assert not self.collection._apply_query(query, {'foo': 'foo'})

    def test_apply_query_honors_logical_and_operators(self):
        # 'bar' must be 'baz', and 'foo' must be an even number 0-10 or an odd number > 10
        query = {
            'bar': 'baz',
            '$or': [
                {'foo': {'$gte': 0, '$lte': 10, '$mod': [2, 0]}},
                {'foo': {'$gt': 10, '$mod': [2, 1]}},
            ]
        }

        assert self.collection._apply_query(query, {'bar': 'baz', 'foo': 4})
        assert self.collection._apply_query(query, {'bar': 'baz', 'foo': 15})
        assert not self.collection._apply_query(query, {'bar': 'baz', 'foo': 14})
        assert not self.collection._apply_query(query, {'bar': 'qux', 'foo': 4})

    def test_apply_query_exists(self):
        query_exists = {'foo': {'$exists': True}}
        query_not_exists = {'foo': {'$exists': False}}

        assert self.collection._apply_query(query_exists, {'foo': 'bar'})
        assert self.collection._apply_query(query_not_exists, {'bar': 'baz'})
        assert not self.collection._apply_query(query_exists, {'baz': 'bar'})
        assert not self.collection._apply_query(query_not_exists, {'foo': 'bar'})

    def test_apply_query_exists_raises(self):
        query = {'foo': {'$exists': 'foo'}}

        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {'foo': 'bar'})

    def test_distinct(self):
        docs = [
            {'foo': 'bar'},
            {'foo': 'baz'},
            {'foo': 10},
            {'bar': 'foo'}
        ]
        self.collection.find = lambda: docs

        assert set(('bar', 'baz', 10)) == self.collection.distinct('foo')

    def test_rename_raises_for_collision(self):
        nosqlite.Collection(self.db, 'bar')  # Create a collision point
        self.collection.create()

        with raises(AssertionError):
            self.collection.rename('bar')

    def test_rename(self):
        self.collection.create()
        assert self.collection.exists()

        self.collection.rename('bar')
        assert self.collection.name == 'bar'
        assert self.collection.exists()

        assert not nosqlite.Collection(self.db, 'foo', create=False).exists()
