"""
used to create, update and prune comments entered by the user.
"""

from datetime import datetime, timedelta
import logging
import sqlite3
import json

log = logging.getLogger(__name__)


def row2dict(row):
    key, timestamp, data = row
    d = json.loads(data)
    d['KEY'] = key
    d['TS'] = timestamp
    return d


class Quote(object):
    """key-value store for Quote of the Day (QOTD) app"""

    def __init__(self, dbfile):

        self.conn = sqlite3.connect(dbfile)
        self._create()
        # By default, SQLite only executes the INSERT trigger when a REPLACE statement is used to replace an existing row. 
        # Set recursive_triggers pragma to on to execute both the DELETE and INSERT triggers when a REPLACE statement is used.
        self.conn.execute('PRAGMA recursive_triggers = on;')

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.close()

    def _create(self):
        """
        Create SQLite tables, index and triggers if not already there.
        """
        cur = self.conn.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS quote '
            '(key TEXT PRIMARY KEY, '
            'timestamp DATE DEFAULT CURRENT_TIMESTAMP, '
            'data BLOB)')
        cur.execute(
            'CREATE INDEX IF NOT EXISTS ix_timestamp_quote ON quote(timestamp)')

        cur.execute(
            'CREATE TABLE IF NOT EXISTS quote_log ( '
            'id integer PRIMARY KEY AUTOINCREMENT, '
            'key_new TEXT, '
            'key_old TEXT, '
            'timestamp_new DATE, '
            'timestamp_old DATE, '
            'data_new TEXT, '
            'data_old TEXT, '
            'sql_action VARCHAR(10), '
            'timestamp DATE DEFAULT CURRENT_TIMESTAMP)')
        cur.execute(
            'CREATE INDEX IF NOT EXISTS ix_timestamp_quote_log ON quote_log(timestamp)')
        cur.execute(
            "CREATE TRIGGER IF NOT EXISTS update_quote_log AFTER UPDATE ON quote "
            "BEGIN "
            "  INSERT INTO quote_log (key_new, key_old, timestamp_new, timestamp_old, data_new, "
            "                         data_old, sql_action) "
            "         VALUES (new.key, old.key, new.timestamp, old.timestamp, new.data, "
            "	  	      old.data, 'UPDATE'); "
            "END")
        cur.execute(
            "CREATE TRIGGER IF NOT EXISTS insert_quote_log AFTER INSERT ON quote "
            "BEGIN "
            "  INSERT INTO quote_log (key_new, timestamp_new, data_new, "
            "                         sql_action) "
            "         VALUES (new.key, new.timestamp, new.data, 'INSERT'); "
            "END")
        cur.execute(
            "CREATE TRIGGER IF NOT EXISTS delete_quote_log BEFORE DELETE ON quote "
            "BEGIN "
            "  INSERT INTO quote_log (key_old, timestamp_old, data_old, "
            "                         sql_action) "
            "         VALUES (old.key, old.timestamp, old.data, 'DELETE'); "
            "END")

    def put(self, key, **kwargs):
        """
        Write key and data to the database.
        """
        cur = self.conn.cursor()
        cmd = ('INSERT OR REPLACE INTO quote '
               '(key, data) '
               'VALUES (?, ?)')
        cur.execute(cmd, (key, json.dumps(kwargs)))
        self.conn.commit()

    def get(self, key, default=None):
        """
        Retrieve details for a specific quote.
        """

        cur = self.conn.cursor()
        cmd = ('SELECT '
               'key, '
               'datetime(timestamp, "localtime") as timestamp, '
               'data '
               'FROM quote '
               'WHERE key = ?')

        cur.execute(cmd, (key,))
        row = cur.fetchone()
        return row2dict(row) if row else default

    def values(self):
        """
        Fetch all key, timestamp, data values. Returns a dict.
        """
        cur = self.conn.cursor()
        cmd = ('SELECT '
               'key, '
               'datetime(timestamp, "localtime") as timestamp, '
               'data '
               'FROM quote')

        cur.execute(cmd)
        return (row2dict(row) for row in cur.fetchall())

    def keys(self):
        """
        Fetch all keys in DB. Returns as a list.
        """
        cur = self.conn.cursor()
        cur.execute('SELECT key FROM quote')
        return [row[0] for row in cur.fetchall()]

    def drop(self, key):
        """
        Delete a specific comment or banner.
        """
        cur = self.conn.cursor()
        cur.execute('DELETE FROM quote WHERE key = ?', (key,))
        self.conn.commit()

    def prune(self, **kwargs):
        """Drop rows with timestamp older than the interval specified
        by a timedelta constructed using ``**kwargs``

        Example (drop records older than 1 day, 5 hours):

        >> db.prune(days=1, hours=5
        """

        dt = datetime.now() - timedelta(**kwargs)
        cmd = "DELETE FROM quote WHERE timestamp <= datetime(:dt)"

        cur = self.conn.cursor()
        cur.execute(cmd, {'dt': dt.strftime('%Y-%m-%d %H:%M:%S')})
        self.conn.commit()

        # Prune the quote_log records too.
        log_cmd = "DELETE FROM quote_log WHERE timestamp <= datetime(:dt)"
        log_cur = self.conn.cursor()
        log_cur.execute(log_cmd, {'dt': dt.strftime('%Y-%m-%d %H:%M:%S')})
        self.conn.commit()
        return (cur.rowcount, log_cur.rowcount)
