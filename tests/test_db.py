import unittest
import tempfile
import os
from datetime import datetime, timedelta
from db import Quote

class TestQuote(unittest.TestCase):

    def setUp(self):
        self.dbfile = tempfile.mktemp()
        self.quote = Quote(self.dbfile)

    def tearDown(self):
        self.quote.close()
        os.remove(self.dbfile)

    def test_put_and_get(self):
        key = 'quote1'
        data = {'author': 'Caterpillar', 'quote': 'Who are you?'}
        self.quote.put(key, **data)
        result = self.quote.get(key)
        self.assertEqual(result, {'KEY': key, 'TS': result['TS'], 'author': 'Caterpillar', 'quote': 'Who are you?'})

    def test_values(self):
        key1 = 'quote1'
        data1 = {'author': 'Caterpillar', 'quote': 'Who are you?'}
        key2 = 'quote2'
        data2 = {'author': 'Mad Hatter', 'quote': 'We are all mad here.'}
        self.quote.put(key1, **data1)
        self.quote.put(key2, **data2)
        result = list(self.quote.values())
        self.assertEqual(len(result), 2)

    def test_keys(self):
        key1 = 'quote1'
        data1 = {'author': 'Caterpillar', 'quote': 'Who are you?'}
        key2 = 'quote2'
        data2 = {'author': 'Mad Hatter', 'quote': 'We are all mad here.'}
        self.quote.put(key1, **data1)
        self.quote.put(key2, **data2)
        result = self.quote.keys()
        self.assertEqual(len(result), 2)
        self.assertIn(key1, result)
        self.assertIn(key2, result)

    def test_drop(self):
        key = 'quote1'
        data = {'author': 'Caterpillar', 'quote': 'Who are you?'}
        self.quote.put(key, **data)
        self.quote.drop(key)
        result = self.quote.get(key)
        self.assertIsNone(result)

    def test_prune(self):
        key1 = 'quote1'
        data1 = {'author': 'Caterpillar', 'quote': 'Who are you?'}
        key2 = 'quote2'
        data2 = {'author': 'Mad Hatter', 'quote': 'We are all mad here.'}
        self.quote.put(key1, **data1)
        self.quote.put(key2, **data2)
        # Manually set the timestamp of the records to a date older than 1 day
        dt = datetime.now() - timedelta(days=2)
        cmd = "UPDATE quote SET timestamp = datetime(:dt) WHERE key IN (:key1, :key2)"
        cur = self.quote.conn.cursor()
        cur.execute(cmd, {'dt': dt.strftime('%Y-%m-%d %H:%M:%S'), 'key1': key1, 'key2': key2})
        self.quote.conn.commit()
        # Prune records older than 1 day
        self.quote.prune(days=1)
        result = list(self.quote.values())
        self.assertEqual(len(result), 0)

if __name__ == '__main__':
    unittest.main()