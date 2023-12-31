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

    def run_ddl(self, query: str, params: tuple = None):
        with self.connection.cursor() as curs:
            curs.execute(query, params)
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

    def is_table_exists(self, table_name=None):
        is_table_exists = False
        if table_name is None:
            table_name = self.table_name
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
            query = sql.SQL('DROP TABLE {table_name} CASCADE').format(
                table_name=sql.Identifier(table_name)
            )
            self.run_ddl(query)

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
    __tablename__ = 'person'

    def __init__(self, connection: pg._psycopg.connection, create_table: bool = False):
        super(Person, self).__init__(connection, create_table, Person.__tablename__)

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

    def delete(self, row_id):
        # Удаление телефонов клиента
        query = sql.SQL("DELETE FROM {person} where person = {person_id} RETURNING id").format(
            person=sql.Identifier(Phone.__tablename__),
            person_id=sql.Placeholder()
        )
        phones = super().run_dml(query, (row_id,))

        # Удаление клиента
        res = super().delete(row_id)
        return res

    def search(self, first_name=None, last_name=None, email=None, phone=None):
        args = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone_number": phone
        }
        conditions = ["{field} = %s".format(field=field) for field, value in args.items() if value is not None]
        conditions = " AND ".join(conditions)
        q = "SELECT p.id, p.first_name, p.last_name, p.email"\
            "  FROM {person} p"
        q += "       JOIN " + Phone.__tablename__ + "  ph on ph.person = p.id "if args['phone_number'] is not None else ""
        q += " WHERE {conditions} " if len(conditions) != 0 else ""
        queue = sql.SQL(q).format(
            person=sql.Identifier(Person.__tablename__),
            conditions=sql.SQL(conditions)
        )
        res = super().run_dml(queue, tuple([value for value in args.values() if value is not None]))
        return res


class Phone(Table):
    __tablename__ = 'phone_number'

    def __init__(self, connection: pg._psycopg.connection, create_table: bool = False):
        super(Phone, self).__init__(connection, create_table, Phone.__tablename__)

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
        return phone_id

    def update(self, row_id: int, phone_number: str = None, person_id: int = None):
        user_id = super().update(row_id, phone_number=phone_number, person=person_id)
        return user_id

    def delete_by_person(self, person_id):
        query = sql.SQL("DELETE FROM {person} where person = {person_id}").format(
            person=sql.Identifier(Person.__tablename__),
            person_id=sql.Placeholder()
        )
        res = super().run_dml(query, (person_id,))
        return res

    @staticmethod
    def __get_conditions__(person_id, phone):
        if person_id is None and phone is None:
            return None
        conditions = "person = %s" if person_id is not None else ""
        conditions += " AND " if person_id and phone else ""
        conditions += "phone_number = %s" if phone is not None else ""
        return conditions, tuple([el for el in [person_id, phone] if el is not None])

    def delete_by_fields(self, person_id=None, phone=None):
        conditions, params = Phone.__get_conditions__(person_id, phone)
        if conditions is None:
            return None
        query = sql.SQL("DELETE FROM {phones} where {conditions} RETURNING id").format(
            phones=sql.Identifier(self.table_name),
            conditions=sql.SQL(conditions)
        )

        res = super().run_dml(query, params)
        return [item[0] for item in res]

    def search(self, person_id=None, phone=None):
        conditions, params = Phone.__get_conditions__(person_id, phone)
        if conditions is None:
            return None

        query = sql.SQL("SELECT id, phone_number, person FROM {phones} where {conditions}").format(
            phones=sql.Identifier(self.table_name),
            conditions=sql.SQL(conditions)
        )

        res = super().run_dml(query, params)
        return res

