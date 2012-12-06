def add_fkey(cursor, table, column, target_table, target_column='id'):
    cursor.execute('''
        alter table %s add constraint %s_%s_fkey
            foreign key (%s) references %s (%s);
    ''' % (
        table,
        table, column,
        column,
        target_table,
        target_column,
    ))
