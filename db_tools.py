import psycopg2 as pg
from psycopg2.extras import NamedTupleCursor

import exceptions as ex


class Table:
    def __init__(self, connection: pg._psycopg.connection, create_table: bool = False, table_name: str = None):
        if not isinstance(connection, pg._psycopg.connection):
            raise ex.DbConnectionError()
        self.connection = connection
        self.table_name = table_name

        is_exists_table = Table.is_table_exists(self.table_name)
        if create_table and is_exists_table:
            raise ex.DbTableAlreadyExists(table_name=table_name)
        if not create_table and not is_exists_table:
            raise ex.DbTableIsNotExists(table_name=table_name)

    def run_dml(connection, query):
        with connection.cursor(cursor_factory=NamedTupleCursor) as curs:
            curs.execute(query)
            connection.commit()

    def is_table_exists(connection, table_name):
        is_table_exists = False
        with connection.cursor(cursor_factory=NamedTupleCursor) as curs:
            curs.execute('SELECT count(1)'
                         '  FROM information_schema.tables'
                         f' WHERE table_name LIKE \'{table_name}\''
                         '   AND table_schema NOT IN (\'information_schema\', \'pg_catalog\')'
                         '   AND table_type = \'BASE TABLE\''
                         ' LIMIT 1;')
            is_table_exists = curs.fetchone()[0]
        return is_table_exists

    def drop_table(connection, table_name):



class Person(Table):
    def __init__(self, connection: pg._psycopg.connection, create_table: bool = False):
        super(Person, self).__init__(connection, create_table, 'person')

        if create_table:
            with self.connection.cursor(cursor_factory=NamedTupleCursor) as curs:
                curs.execute('CREATE TABLE person ('
                             'id SERIAL      PRIMARY KEY,'
                             'first_name     VARCHAR(60) NOT NULL,'
                             'last_name      VARCHAR(60) NOT NULL,'
                             'email          VARCHAR(100) NOT NULL'
                             ');')
                connection.commit()

    def insert(self, first_name: str, last_name: str, email: str, phones: list) -> int:
        pass

    def delete(self):
        pass

    def update(self, row_id: int):
        pass

    def search(self, **kwargs):
        pass

class Phone(Table):


    def __init__(self, connection: pg._psycopg.connection, create_table: bool = False):
        super(Phone, self).__init__(connection, create_table, 'phone_number')

        if create_table:
            with self.connection.cursor(cursor_factory=NamedTupleCursor) as curs:
                curs.execute('CREATE TABLE phone_number ('
                             '    id             SERIAL      PRIMARY KEY,'
                             '    phone_number   varchar(16) NOT NULL,'
                             '    person         integer     NOT NULL REFERENCES person(id)'
                             ');')
                connection.commit()

    def insert(self):
        pass

    def delete(self):
        pass

    def update(self, row_id: int):
        pass

    def search(self, **kwargs):
        pass

