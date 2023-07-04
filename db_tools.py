import re

import psycopg2 as pg
from psycopg2 import sql
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

    def run_ddl(self, query: str):
        with self.connection.cursor() as curs:
            curs.execute(query)
            self.connection.commit()

    def run_dml(self, query: str, params: tuple):
        res = None
        with self.connection.cursor() as curs:
            curs.execute(query, params)
            res = curs.fetchall()
            self.connection.commit()
        return res

    def create_table(self):
        if self.is_table_exists(self.table_name):
            raise ex.DbTableAlreadyExists(table_name=self.table_name)

    def is_table_exists(self, table_name):
        is_table_exists = False
        with self.connection.cursor() as curs:
            curs.execute('SELECT count(1)'
                         '  FROM information_schema.tables'
                         f' WHERE table_name = %s'
                         '   AND table_schema NOT IN (\'information_schema\', \'pg_catalog\')'
                         '   AND table_type = \'BASE TABLE\''
                         ' LIMIT 1;', (table_name,))
            is_table_exists = curs.fetchall()
        return is_table_exists[0][0] == 1

    def drop_table(self, table_name: str = None):
        if table_name is None:
            table_name = self.table_name
        if self.is_table_exists(table_name):
            query = f'DROP TABLE %s'
            self.run_ddl(query, (table_name,))

    def insert(self, **fields) -> int:
        col_names = sql.SQL(', ').join(sql.Identifier(field) for field in fields.keys())
        place_holders = sql.SQL(', ').join(sql.Placeholder() * len(fields.keys()))

        query = sql.SQL("INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING id").format(
            table=sql.Identifier(self.table_name),
            columns=col_names,
            values=place_holders
        )
        values = tuple(fields.values())
        user_id = self.run_dml(query, values)
        return user_id[0][0]

    def delete(self, row_id):
        query = sql.SQL("DELETE FROM {table_name} WHERE id = {id} RETURNING id").format(
            table_name=sql.Identifier(self.table_name),
            id=sql.Placeholder()
        )
        delete_id = self.run_dml(query, (row_id,))
        return delete_id[0][0]

    def update(self, row_id: int, **fields):
        query = sql.SQL("UPDATE {table_name} SET {column_values} WHERE id = {id} RETURNING id").format(
            table_name=sql.Identifier(self.table_name),
            column_values=sql.SQL(",\n").join([
                                sql.SQL("{field} = {value}").format(
                                    field=sql.Identifier(key),
                                    value=sql.Placeholder()
                                ) for key in fields.keys() if fields[key] is not None
                            ]),
            id=sql.Placeholder()
        )
        args = list(value for value in fields.values() if value is not None)
        args += str(row_id)
        updated_id = self.run_dml(query, tuple(args))
        return updated_id[0][0]

    def select(self, ):
        pass


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
        self.run_ddl(query)

    def insert(self, first_name: str, last_name: str, email: str) -> int:
        user_id = super().insert(first_name=first_name, last_name=last_name, email=email)
        return user_id

    def update(self, row_id: int, first_name: str = None, last_name: str = None, email: str = None) -> list:
        user_id = super().update(row_id, first_name=first_name, last_name=last_name, email=email)
        return user_id

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
        self.run_ddl(query)

    @staticmethod
    def __check_phone__(phone):
        """
        Check, is phone match pattern +79999999999 - 11 numbers and '+' in position 0
        :param phone: str
        :return: bool
        """
        return len(phone) == 12 and re.match(r"\+7\d{10}", phone)

    def insert(self, phone_number: str, person_id: int):
        if not Phone.__check_phone__(phone_number):
            raise ex.DbColumnTypeError(f'Phone number {phone_number} does not match pattern +79999999999')
        phone_id = super().insert(phone_number=phone_number, person=person_id)
        return phone_id[0][0]

    def update(self, row_id: int, phone_number: str = None, person_id: int = None):
        user_id = super().update(row_id, phone_number=phone_number, person=person_id)
        return user_id[0][0]

    def search(self, **kwargs):
        pass

