import psycopg2
import re

from . import dtuple

class ExpressionValue(str):
    pass

class ExpressionValueAdapter(object):
    def __init__(self, value):
        self.value = value
    
    def getquoted(self):
        return str(self.value)

psycopg2.extensions.register_adapter(ExpressionValue, ExpressionValueAdapter)

# Quoting of table/schema names in psycopg2:
# http://osdir.com/ml/python.db.psycopg.devel/2004-10/msg00012.html
class SchemaName(str):
    pass

class SchemaNameAdapter(object):
    def __init__(self, value):
        self.value = value
    
    def getquoted(self):
        return '"' + self.value.replace('"', '""') + '"'

psycopg2.extensions.register_adapter(SchemaName, SchemaNameAdapter)

# Use this for obtaining original psycopg2 tuple/list escaping behavior
class SqlArray(list):
    pass

psycopg2.extensions.register_adapter(list, psycopg2.extensions.SQL_IN)
psycopg2.extensions.register_adapter(SqlArray, psycopg2._psycopg.List)

WHITESPACE_REGEXP = re.compile(r'^\s+', re.M)

class NotFoundError(StandardError):
    pass

class CursorWrapper:
    def __init__(self, cursor, conn, debug=False):
        self.cursor = cursor
        self.conn = conn
        self._transaction_depth = 0
        self._transaction_depth_request = 0
        self._debug = debug
    
    def execute(self, sql, *args):
        return self.execute2(sql, args)
    
    def execute2(self, sql, args):
        sql = sql.replace('?', '%s')
        
        if args is None:
            args = ()
        elif isinstance(args, dict):
            # keep as a dict
            pass
        elif isinstance(args, basestring) or getattr(args, '__len__', None) is None:
            args = (args,)
        
        try:
            if self._debug:
                debug_sql = WHITESPACE_REGEXP.sub('     ', sql.strip())
                if args:
                    debug_sql += ', ' + repr(args)
                print 'SQL:', debug_sql
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
                self.execute2(sql)
    
    #def fetchall(self):
        #return self.cursor.fetchall()
    
    # Higher-level interface
    
    # one/etc. take args as arguments.
    # to pass additional options use one2/etc.
    
    def one(self, sql, *args):
        return self.one2(sql, args)
    
    def one2(self, sql, args):
        self.execute2(sql, args)
        row = self.cursor.fetchone()
        if row is None:
            return None
        desc = dtuple.TupleDescriptor(self.cursor.description)
        row = dtuple.DatabaseTuple(desc, row)
        return row
    
    def one_check(self, sql, *args):
        return self.one_check2(sql, args)
    
    def one_check2(self, sql, args):
        row = self.one2(sql, args)
        if row is None:
            raise NotFoundError, "No data"
        return row
    
    def all(self, sql, *args):
        return self.all2(sql, args)
    
    def all2(self, sql, args):
        self.execute2(sql, args)
        desc = dtuple.TupleDescriptor(self.cursor.description)
        rows = self.cursor.fetchall()
        rows = [dtuple.DatabaseTuple(desc, row) for row in rows]
        return rows
    
    def one_value(self, sql, *args):
        return self.one_value2(sql, args)
    
    def one_value2(self, sql, args):
        self.execute2(sql, args)
        row = self.cursor.fetchone()
        if row is None:
            return None
        return row[0]
    
    def one_value_check(self, sql, *args):
        return self.one_value_check2(sql, args)
    
    def one_value_check2(self, sql, args):
        self.execute2(sql, args)
        row = self.cursor.fetchone()
        if row is None:
            raise NotFoundError, "No data"
        return row[0]
    
    def all_values(self, sql, *args):
        return self.all_values2(sql, args)
    
    def all_values2(self, sql, args):
        self.execute2(sql, args)
        desc = dtuple.TupleDescriptor(self.cursor.description)
        rows = self.cursor.fetchall()
        rows = [row[0] for row in rows]
        return rows
    
    # Transactions
    
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
    
    # Statements
    
    #def insert_dict(self, table, dict):
    #    table = quote_name(table)
    #    keys_str = ','.join(map(quote_name, dict.keys()))
    #    values = []
    #    values_placeholders= []
    #    for value in dict.values():
    #        if isinstance(value, ExpressionValue):
    #            values_placeholders.append(value)
    #        else:
    #            values_placeholders.append('%s')
    #            values.append(value)
    #    values_str = ','.join(values_placeholders)
    #    sql = 'insert into %s (%s) values (%s)' % (table, keys_str, values_str)
    #    self.execute(sql, values)
    
    def insert_dict(self, table, dict):
        return self._insert_dict_impl(table, dict, False)
    
    def insert_dict_id(self, table, dict):
        return self._insert_dict_impl(table, dict, True)
    
    def _insert_dict_impl(self, table, dict, return_id):
        if not dict:
            raise ValueError, 'Cannot insert an empty dict'
        placeholders = ', '.join(['%s'] * len(dict))
        sql = 'insert into %%s (%s) values (%s)' % (placeholders, placeholders)
        if return_id:
            sql += ' returning (id)'
        table = SchemaName(table)
        columns = [SchemaName(column) for column in dict.keys()]
        values = dict.values()
        args = [table] + columns + values
        self.execute(sql, *args)
        if return_id:
            row = self.cursor.fetchone()
            return row[0]
    
    def update(self, table, attrs, conditions):
        if not attrs:
            raise ValueError, 'Trying to update with an empty attrs'
        placeholders = ', '.join(['%s=%s'] * len(attrs))
        sql = 'update %s set ' + placeholders
        args = [SchemaName(table)]
        for key in attrs:
            args.append(SchemaName(key))
            args.append(attrs[key])
        if conditions is not None:
            sql += ' where '
            if isinstance(conditions, str):
                sql += conditions
            elif isinstance(conditions, dict):
                if not conditions:
                    raise ValueError, 'Conditions was an empty dict'
                sql += ' and '.join(['%s=%s'] * len(conditions))
                for key in conditions:
                    args.append(SchemaName(key))
                    args.append(conditions[key])
            elif isinstance(conditions, tuple) or isinstance(conditions, list):
                sql += conditions[0]
                args += conditions[1:]
            else:
                raise ValueError, "Don't know what to do with these conditions"
        print sql, args
        self.execute(sql, *args)
    
    # DDL statements
    
    def add_fkey(self, table, column, target_table=None, target_column=None):
        target_table = column[:-3] + 's'
        target_column = 'id'
        name = '%s_%s_fk' % (table, column)
        vars = {
            'table': table,
            'column': column,
            'name': name,
            'target_table': target_table,
            'target_column': target_column,
        }
        self.execute('''
            alter table %(table)s add constraint %(name)s
                foreign key (%(column)s)
                references %(target_table)s (%(target_column)s)
        ''' % vars)

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
    def __init__(self, dsn, debug=False):
        self.dsn = dsn
        self.conn = None
        self._debug = debug
    
    def cursor(self):
        cursor = CursorWrapper(self.conn.cursor(), self, debug=self._debug)
        return CursorContextManager(cursor)
    
    def tx_cursor(self):
        cursor = CursorWrapper(self.conn.cursor(), self, debug=self._debug)
        return TransactionalCursorContextManager(cursor)
    
    # we need to rollback transactions after failed statements
    cursor = tx_cursor
    
    def commit(self):
        return self.conn.commit()
    
    def rollback(self):
        return self.conn.rollback()
    
    def connect(self):
        self.conn = psycopg2.connect(self.dsn)
    
    def reconnect(self):
        self.connect()
    
    def expr(self, value):
        return ExpressionValue(value)
