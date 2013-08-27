import psycopg2, psycopg2.extras, psycopg2.extensions
import re

from . import dtuple

# Receive strings from the database in unicode
# http://initd.org/psycopg/docs/usage.html#unicode-handling
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

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

#psycopg2.extensions.register_adapter(list, psycopg2.extensions.SQL_IN)
psycopg2.extensions.register_adapter(SqlArray, psycopg2._psycopg.List)

WHITESPACE_REGEXP = re.compile(r'^\s+', re.M)

class DatabaseError(StandardError):
    pass

class NotFoundError(DatabaseError):
    pass

class TransactionStateError(DatabaseError):
    pass

class MissingCursorDescriptionError(DatabaseError):
    pass

class DatabaseConnectionClosed(DatabaseError):
    pass

def _lists_to_tuples(arg):
    if isinstance(arg, list):
        arg = tuple(arg)
    return arg

class CachingCursorWrapper(object):
    def __init__(self, cursor):
        self.cursor = cursor
        self.cache = {}
    
    def mogrify(self, sql, args):
        return self.cursor.mogrify(sql, args)
    
    def execute(self, sql, args):
        key = (sql, repr(args))
        if key in self.cache:
            self.result = self.cache[key]
            self.executing = False
        else:
            self.cursor.execute(sql, args)
            self.result = self.cache[key] = {}
            self.executing = True
    
    def populate_result(self):
        rows = self.cursor.fetchall()
        description = self.cursor.description
        self.result['rows'] = rows
        self.result['description'] = description
        self.executing = False
    
    @property
    def description(self):
        if self.executing:
            self.populate_result()
        return self.result['description']
    
    def fetchall(self):
        if self.executing:
            self.populate_result()
        return self.result['rows']
    
    def fetchone(self):
        if self.executing:
            self.populate_result()
        rows = self.result['rows']
        if rows:
            return rows[0]
        else:
            return None
    
    def close(self):
        self.cursor.close()

class CursorWrapper(object):
    def __init__(self, cursor, conn,
        debug_queries=False, debug_transactions=False,
    ):
        self.cursor = cursor
        self.conn = conn
        self._debug_queries = debug_queries
        self._debug_transactions = debug_transactions
    
    def execute(self, sql, *args):
        return self.execute2(sql, args)
    
    def execute2(self, sql, args, munge=False):
        sql = sql.replace('?', '%s')
        
        convert_lists = False
        
        if args is None:
            args = ()
        elif isinstance(args, dict):
            # keep as a dict
            # convert lists to tuples
            for key in args:
                value = args[key]
                if isinstance(value, list):
                    args[key] = tuple(value)
        elif isinstance(args, basestring) or getattr(args, '__len__', None) is None:
            args = (args,)
            convert_lists = True
        else:
            # a tuple or a list
            convert_lists = True
        
        if convert_lists:
            args = map(_lists_to_tuples, args)
        
        if munge:
            sql, self._munge_mapping = _munge_sql(sql)
        else:
            self._munge_mapping = None
        
        try:
            if self._debug_queries:
                debug_sql = self.cursor.mogrify(sql, args)
                #debug_sql = sql.strip()
                #if args:
                    #debug_sql += ', ' + repr(args)
                debug_sql = WHITESPACE_REGEXP.sub('     ', debug_sql.strip())
                print 'SQL:', debug_sql
            self.cursor.execute(sql, args)
        except psycopg2.OperationalError, e:
            if str(e).startswith('server closed the connection unexpectedly'):
                if self.conn._transaction_depth == 0:
                    self.conn.reconnect()
                    self.cursor = self.conn.cursor()
                    self.cursor.execute(sql, args)
                else:
                    self.conn.want_reconnect = True
                    raise DatabaseConnectionClosed
            else:
                raise
        
        #if self.conn._transaction_depth_request:
            #self.conn._transaction_depth += 1
            #self.conn._transaction_depth_request -= 1
    
    def execute_many(self, sql_commands):
        for sql in sql_commands.split(';'):
            sql = sql.strip()
            if sql:
                self.execute2(sql, ())
    
    #def fetchall(self):
        #return self.cursor.fetchall()
    
    # Higher-level interface
    
    # one/etc. take args as arguments.
    # to pass additional options use one2/etc.
    
    def one(self, sql, *args):
        return self.one2(sql, args)
    
    def onem(self, sql, *args):
        return self.one2(sql, args, munge=True)
    
    def one2(self, sql, args, **kwargs):
        self.execute2(sql, args, **kwargs)
        row = self.cursor.fetchone()
        if row is None:
            return None
        desc = dtuple.TupleDescriptor(self.cursor.description)
        row = dtuple.DatabaseTuple(desc, row)
        if self._munge_mapping:
            row = _munge_row(dict(row), self._munge_mapping)
        return row
    
    def one_check(self, sql, *args):
        return self.one_check2(sql, args)
    
    def one_checkm(self, sql, *args):
        return self.one_check2(sql, args, munge=True)
    
    def one_check2(self, sql, args, **kwargs):
        row = self.one2(sql, args, **kwargs)
        if row is None:
            raise NotFoundError, "No data %s" % repr(args)
        return row
    
    def all(self, sql, *args):
        return self.all2(sql, args)
    
    def allm(self, sql, *args):
        return self.all2(sql, args, munge=True)
    
    def all2(self, sql, args, **kwargs):
        self.execute2(sql, args, **kwargs)
        if self.cursor.description is None:
            raise MissingCursorDescriptionError
        desc = dtuple.TupleDescriptor(self.cursor.description)
        rows = self.cursor.fetchall()
        rows = [dtuple.DatabaseTuple(desc, row) for row in rows]
        if self._munge_mapping:
            # XXX dispose of dict() call
            rows = [_munge_row(dict(row), self._munge_mapping) for row in rows]
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
            raise NotFoundError, "No data %s" % repr(args)
        return row[0]
    
    def all_values(self, sql, *args):
        return self.all_values2(sql, args)
    
    def all_values2(self, sql, args):
        self.execute2(sql, args)
        desc = dtuple.TupleDescriptor(self.cursor.description)
        rows = self.cursor.fetchall()
        rows = [row[0] for row in rows]
        return rows
    
    @property
    def rowcount(self):
        return self.cursor.rowcount
    
    # Transactions
    
    def begin(self):
        self.conn.begin()
    
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback()
    
    # this is used by fixture
    def flush(self):
        self.commit()
        self.begin()
    
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
    
    def list_tables(self):
        return self.all_values('''
            select relname from pg_class
            where relkind='r' and
                relname not like %s
                and relname not like %s
        ''', 'pg_%', 'sql_%')
    
    def list_sequences(self):
        return self.all_values('''
            select relname from pg_class
            where relkind='S' and
                relname not like %s
                and relname not like %s
        ''', 'pg_%', 'sql_%')
    
    def list_functions(self):
        public_namespace = self.one_value_check('''
            select oid from pg_namespace where nspname=?
        ''', 'public')
        plpgsql_language = self.one_value_check('''
            select oid from pg_language where lanname=?
        ''', 'plpgsql')
        return self.all_values('''
            select proname from pg_proc
            where pronamespace=? and prolang=?
        ''', public_namespace, plpgsql_language)

class CursorContextManager(object):
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

class ConnectionWrapper(object):
    def __init__(self, dsn,
        debug_queries=False, debug_transactions=False,
    ):
        self.dsn = dsn
        self.conn = None
        self._debug_queries = debug_queries
        self._debug_transactions = debug_transactions
        self._transaction_depth = 0
        self._transaction_depth_request = 0
        self._rolling_back = False
        self.want_reconnect = False
    
    def cursor(self):
        #cursor = CursorWrapper(self.conn.cursor(), self, debug=self._debug)
        cursor = self.get_cursor()
        return TransactionalCursorContextManager(cursor)
    
    def tx_cursor(self):
        #cursor = CursorWrapper(self.conn.cursor(), self, debug=self._debug)
        cursor = self.get_cursor()
        return TransactionalCursorContextManager(cursor)
    
    def caching_cursor(self):
        cursor = self.get_cursor(CachingCursorWrapper)
        return TransactionalCursorContextManager(cursor)
    
    # cursor returns a context manager, we need a method that returns
    # actual cursor for fixture
    def get_cursor(self, wrapper=None):
        # when we lose the connection at depth > 0,
        # we don't want to reconnect right away as in that case
        # the transaction won't be properly setup.
        # we abort all nested transactions and next time
        # a cursor is retrieved at depth 0 we try to reconnect.
        if self._transaction_depth == 0 and self.want_reconnect:
            self.reconnect()
        
        cursor = self.conn.cursor()
        if wrapper is not None:
            cursor = wrapper(cursor)
        cursor = CursorWrapper(cursor, self,
            debug_queries=self._debug_queries, debug_transactions=self._debug_transactions,
        )
        return cursor
    
    # we need to rollback transactions after failed statements
    cursor = tx_cursor
    
    def begin(self):
        if self._transaction_depth == 0:
            self._rolling_back = False
        self._transaction_depth_request += 1
        self._transaction_depth += 1
        
        if self._debug_transactions:
            print 'BEGIN: %d' % self._transaction_depth
    
    def commit(self):
        if self._debug_transactions:
            print 'COMMIT: %d' % self._transaction_depth
        
        if self._rolling_back:
            raise TransactionStateError, 'Tried to commit after a nested transaction requested a rollback (or was aborted)'
        
        transaction_depth = self._transaction_depth - 1
        transaction_depth_delta = 1
        if transaction_depth == 0:
            if self._debug_transactions:
                print 'COMMITTING'
            
            self.conn.commit()
        elif transaction_depth == -1:
            if self._debug_transactions:
                print 'COMMITTING IMPLICIT TX'
            
            transaction_depth = 0
            transaction_depth_delta = 0
            
            self.conn.commit()
        elif transaction_depth < 0:
            raise TransactionStateError, 'Requested a commit but we are not tracking a transaction in progress'
        else:
            # transaction depth is 0
            pass
        
        self._transaction_depth = transaction_depth
        self._transaction_depth_request -= transaction_depth_delta
    
    def rollback(self):
        if self._debug_transactions:
            print 'ROLLBACK: %d' % self._transaction_depth
        
        transaction_depth = self._transaction_depth - 1
        transaction_depth_delta = 1
        do_rollback = False
        if transaction_depth < 0:
            transaction_depth = 0
            transaction_depth_delta = 0
            do_rollback = True
        elif transaction_depth == 0:
            do_rollback = True
        else:
            self._rolling_back = True
            # rolling back a nested transaction
            #raise NotImplementedError, 'Rollback of nested transactions is not supported'
        
        if do_rollback:
            if self._debug_transactions:
                print 'ROLLING BACK'
            
            self.conn.rollback()
        
        self._transaction_depth = transaction_depth
        self._transaction_depth_request -= transaction_depth_delta
    
    def connect(self):
        self.conn = psycopg2.connect(self.dsn)
        #psycopg2.extras.register_hstore(self.conn)
        self.want_reconnect = False
    
    def reconnect(self):
        self.connect()
    
    def expr(self, value):
        return ExpressionValue(value)

from .dbutils import _munge_sql, _munge_row
