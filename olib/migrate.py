import sys

class MigrationError(StandardError):
    pass

class WrongMigrationName(MigrationError):
    '''Migration name does not match expected pattern'''

class IrreversibleMigration(MigrationError):
    pass

class Migration(object):
    def __init__(self, name, fn, number):
        self.name = name
        self.fn = fn
        self.number = number
    
    @property
    def has_down(self):
        return hasattr(sys.modules[self.fn.__module__], self.name + '_down')
    
    @property
    def down_fn(self):
        return getattr(sys.modules[self.fn.__module__], self.name + '_down')

def find_migrations(migrations_module):
    import types, re
    
    migrations = []
    
    for name in dir(migrations_module):
        if name.startswith('_') or name.endswith('_down'):
            continue
        
        fn = getattr(migrations_module, name)
        if type(fn) is types.FunctionType:
            match = re.match(r'm(\d+)$', name)
            if match is None:
                raise WrongMigrationName, "Bad migration name: %s" % name
            number = int(match.group(1))
            
            migration = Migration(name, fn, number)
            migrations.append(migration)
    
    return migrations

class CursorWrapper(object):
    def __init__(self, cursor):
        self.cursor = cursor
    
    def all_values(self, *args):
        self.cursor.execute(*args)
        rows = self.cursor.fetchall()
        values = [row[0] for row in rows]
        return values
    
    def execute(self, *args):
        return self.cursor.execute(*args)
    
    def commit(self, *args):
        return self.cursor.commit(*args)
    
    def rollback(self, *args):
        return self.cursor.rollback(*args)
    
    def close(self, *args):
        return self.cursor.close(*args)

class DbFacade(object):
    def __init__(self, cursor):
        self.cursor = cursor
    
class MysqlFacade(DbFacade):
    def list_tables(self):
        return self.cursor.all_values('''
            show tables
        ''')
    
    def create_migrations_table(self):
        tables = self.list_tables()
        if not 'migrations' in tables:
            self.cursor.execute('''
                create table migrations (
                    id integer auto_increment primary key,
                    name varchar(100) not null unique key,
                    migrated_at timestamp not null
                );
            ''')
    
    def commit(self):
        return self.cursor.execute('commit')
    
    def rollback(self):
        return self.cursor.execute('rollback')

class PostgresqlFacade(DbFacade):
    def list_tables(self):
        return self.cursor.all_values('''
            select relname from pg_class
            where relkind='r' and
                relname not like %s
                and relname not like %s
        ''', ('pg_%', 'sql_%'))
    
    def create_migrations_table(self):
        tables = self.list_tables()
        if not 'migrations' in tables:
            self.cursor.execute('''
                create table migrations (
                    id serial,
                    name varchar(100) not null,
                    migrated_at timestamp not null,
                    constraint migrations_name_unique unique (name),
                    primary key (id)
                );
            ''')
    
    def commit(self):
        return self.cursor.commit()
    
    def rollback(self):
        return self.cursor.rollback()

def db_facade_factory(dialect):
    this_module = sys.modules[__name__]
    class_name = dialect[0].upper() + dialect[1:] + 'Facade'
    facade_class = getattr(this_module, class_name)
    return facade_class

class Migrator(object):
    def __init__(self, db_facade_class, db_conn):
        self.db_facade_class = db_facade_class
        self.conn = db_conn
    
    def transactional_cursor(self):
        cursor = CursorWrapper(self.conn.cursor())
        facade = self.db_facade_class(cursor)
        return TransactionalCursor(cursor, facade)
    
    def create_migrations_table(self):
        with self.transactional_cursor() as c:
            db_facade = self.db_facade_class(c)
            db_facade.create_migrations_table()

    def migrate(name, fn, dir='up'):
        with self.transactional_cursor() as c:
            migrated = c.one_value('''
                select id from migrations where name=?
            ''', name)
            if dir == 'up' and not migrated:
                fn(self.conn, c.cursor)
                c.execute('''
                    insert into migrations (name, migrated_at)
                    values (?, now())
                ''', name)
            elif dir == 'down' and migrated:
                fn(self.conn, c.cursor)
                c.execute('''
                    delete from migrations where name=?
                ''', name)

def migrate(dialect, db_conn, migrations_module):
    facade_class = db_facade_factory(dialect)
    migrator = Migrator(facade_class, db_conn)
    migrator.create_migrations_table()
    
    migrations = find_migrations(migrations_module)
    
    for migration in migrations:
        migrator.migrate(migration.name, migration.fn)

def execute_down(migration_number):
    migrator = check_and_migrate_via_db
    migrations = find_migrations()
    
    for migration in migrations:
        if migration.number == migration_number:
            if migration.has_down:
                migrator(migration.name, migration.down_fn, db_conn, dir='down')
            else:
                raise IrreversibleMigration, 'No down migration for %d' % migration_number

def erase(migration_number):
    from . import environment
    
    db_conn = environment._db_conn
    with db_conn.tx_cursor() as c:
        c.execute('''
            delete from migrations where name like ?
        ''', ('m%04d%%' % migration_number))

def build_truncate_all_tables_stored_procedure():
    from . import environment
    
    sql = '''
        create or replace function truncate_all_tables() returns void as $$
        begin
    '''
    
    db_conn = environment._db_conn
    with db_conn.cursor() as c:
        tables = c.list_tables()
        for table in tables:
            if table == 'migrations':
                continue
            sql += 'truncate table %s cascade;' % table
    
    sql += '''
        end
        $$ language plpgsql;
    '''
    return sql

class TransactionalCursor(object):
    def __init__(self, cursor, facade):
        self.cursor = cursor
        self.facade = facade
    
    def __enter__(self):
        return self.cursor
    
    def __exit__(self, type, value, traceback):
        if type is None:
            self.facade.commit()
            self.cursor.close()
        else:
            try:
                self.facade.rollback()
            except:
                pass
            try:
                self.cursor.close()
            except:
                pass
