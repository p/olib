import sys
from . import dbwrap

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

def migrate(db_conn, migrations_module):
    create_migrations_table(db_conn)
    
    migrator = check_and_migrate_via_db
    migrations = find_migrations(migrations_module)
    
    for migration in migrations:
        migrator(migration.name, migration.fn, db_conn)

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

def check_and_migrate_via_db(name, fn, db_conn, dir='up'):
    with db_conn.tx_cursor() as c:
        migrated = c.one_value('''
            select id from migrations where name=?
        ''', name)
        if dir == 'up' and not migrated:
            fn(db_conn)
            c.execute('''
                insert into migrations (name, migrated_at)
                values (?, now())
            ''', name)
        elif dir == 'down' and migrated:
            fn(db_conn)
            c.execute('''
                delete from migrations where name=?
            ''', name)

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

def create_migrations_table(conn):
    with transactional_cursor(conn) as c:
        tables = c.list_tables()
        if not 'migrations' in tables:
            c.execute('''
                create table migrations (
                    id serial,
                    name varchar(100) not null,
                    migrated_at timestamp not null,
                    constraint migrations_name_unique unique (name),
                    primary key (id)
                );
            ''')

class transactional_cursor(object):
    def __init__(self, conn):
        self.cursor = dbwrap.CursorWrapper(conn.cursor(), conn)
    
    def __enter__(self):
        return self.cursor
    
    def __exit__(self, type, value, traceback):
        if type is None:
            self.cursor.commit()
            self.cursor.close()
        else:
            try:
                self.cursor.rollback()
            except:
                pass
            try:
                self.cursor.close()
            except:
                pass
