import psycopg2

from . import dtuple

class CursorWrapper:
    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn
        self._transaction_depth = 0
        self._transaction_depth_request = 0
    
    def execute(self, sql, args=None):
        sql = sql.replace('%', '%%').replace('?', '%s')
        
        try:
            return self.cursor.execute(sql, args)
        except psycopg2.OperationalError, e:
            if str(e).startswith('server closed the connection unexpectedly'):
                if self._transaction_depth == 0:
                    self.conn.reconnect()
                    self.cursor = self.conn.cursor()
                    self.cursor.execute(sql, args)
            else:
                raise
        
        if self._transaction_depth_request:
            self._transaction_depth += 1
            self._transaction_depth_request -= 1
    
    def execute_many(self, sql_commands):
        for sql in sql_commands.split(';'):
            sql = sql.strip()
            if sql:
                self.execute(sql)
    
    def fetchall(self):
        return self.cursor.fetchall()
    
    def all(self, sql, args=None):
        self.execute(sql, args)
        desc = dtuple.TupleDescriptor(self.cursor.description)
        rows = self.cursor.fetchall()
        rows = [dtuple.DatabaseTuple(desc, row) for row in rows]
        return rows
    
    def begin(self):
        self._transaction_depth_request += 1
    
    def commit(self):
        self.conn.commit()
        self._transaction_depth -= 1
    
    def rollback(self):
        self.conn.rollback()
        self._transaction_depth -= 1
    
    def close(self):
        return self.cursor.close()

class CursorContextManager:
    def __init__(self, cursor):
        self.cursor = cursor
    
    def __enter__(self):
        return self.cursor
    
    def __exit__(self, type, value, traceback):
        self.cursor.close()

class TransactionalCursorContextManager(CursorContextManager):
    def __enter__(self):
        self.cursor.begin()
        return CursorContextManager.__enter__(self)
    
    def __exit__(self, type, value, traceback):
        if value is None:
            self.cursor.commit()
        else:
            self.cursor.rollback()
        return CursorContextManager.__exit__(self, type, value, traceback)

class ConnectionWrapper:
    def __init__(self, dsn):
        self.dsn = dsn
        self.conn = None
    
    def cursor(self):
        cursor = CursorWrapper(self.conn.cursor())
        return CursorContextManager(cursor)
    
    def tx_cursor(self):
        cursor = CursorWrapper(self.conn.cursor(), self)
        return TransactionalCursorContextManager(cursor)
    
    def commit(self):
        return self.conn.commit()
    
    def rollback(self):
        return self.conn.rollback()
    
    def connect(self):
        self.conn = psycopg2.connect(self.dsn)
    
    def reconnect(self):
        self.connect()
