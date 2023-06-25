import psycopg2 as pg
from psycopg2.extras import NamedTupleCursor

import exceptions as ex


class Table:
    def __init__(self, connection: pg._psycopg.connection, create_table: bool = False, table_name: str = None):
        if not isinstance(connection, pg._psycopg.connection):
            raise ex.DbConnectionError()
        self.connection = connection
        self.table_name = table_name

        is_exists_table = self.is_table_exists(table_name)
        if create_table and is_exists_table:
            raise ex.DbTableAlreadyExists(table_name=self.table_name)
        # if not create_table and not is_exists_table:
        #     raise ex.DbTableIsNotExists(table_name=table_name)

        if create_table:
            self.create_table()

    def run_dml(self, query):
        with self.connection.cursor() as curs:
            curs.execute(query)
            self.connection.commit()

    def create_table(self):
        if self.is_table_exists(self.table_name):
            raise ex.DbTableAlreadyExists(table_name=self.table_name)

    def is_table_exists(self, table_name):
        is_table_exists = False
        with self.connection.cursor() as curs:
            curs.execute('SELECT count(1)'
                         '  FROM information_schema.tables'
                         f' WHERE table_name = \'{table_name}\''
                         '   AND table_schema NOT IN (\'information_schema\', \'pg_catalog\')'
                         '   AND table_type = \'BASE TABLE\''
                         ' LIMIT 1;')
            is_table_exists = curs.fetchone()[0]
        return is_table_exists

    def drop_table(self, table_name: str = None):
        if table_name is None:
            table_name = self.table_name
        if self.is_table_exists(table_name):
            query = f'DROP TABLE {table_name}'
            self.run_dml(query)


class Person(Table):
    def __init__(self, connection: pg._psycopg.connection, create_table: bool = False):
        super(Person, self).__init__(connection, create_table, 'person')

        if create_table:
            self.create_table()

    def create_table(self):
        super().create_table()
        query = 'CREATE TABLE person ('\
                '    id SERIAL      PRIMARY KEY,'\
                '    first_name     VARCHAR(60) NOT NULL,'\
                '    last_name      VARCHAR(60) NOT NULL,'\
                '    email          VARCHAR(100) NOT NULL'\
                ');'
        self.run_dml(query)

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

    def create_table(self):
        super().create_table()
        query = 'CREATE TABLE phone_number ('\
                '    id             SERIAL      PRIMARY KEY,'\
                '    phone_number   varchar(16) NOT NULL,'\
                '    person         integer     NOT NULL REFERENCES person(id)'\
                ');'
        self.run_dml(query)

    def insert(self):
        pass

    def delete(self):
        pass

    def update(self, row_id: int):
        pass

    def search(self, **kwargs):
        pass

