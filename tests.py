from mock import Mock, patch
from unittest import TestCase

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
