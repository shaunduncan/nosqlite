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

    def test_has_all_keys_passes(self):
        keys = ('foo', 'bar', 'baz')
        doc = dict.fromkeys(keys, '')
        assert self.collection._has_all_keys(keys, doc)

    def test_has_all_keys_fails(self):
        keys = ('foo', 'bar', 'baz')
        doc = dict.fromkeys(('foo', 'bar'), '')
        assert not self.collection._has_all_keys(keys, doc)

    def test_has_any_key_passes(self):
        doc = dict.fromkeys(('foo', 'bar'), '')
        assert self.collection._has_any_key(('foo', 'baz'), doc)

    def test_has_any_key_fails(self):
        doc = dict.fromkeys(('foo', 'bar'), '')
        assert not self.collection._has_any_key(('qux', 'baz'), doc)

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
        collection._load_document = lambda id, data: data

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
        collection._load_document = lambda id, data: data

        ret = collection.find(query, limit=1)
        assert len(ret) == 1
