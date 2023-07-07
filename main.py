from os import getenv
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import NamedTupleCursor

from db_tools import Person, Phone


def create_db(tables):
    for table in tables:
        if table.is_table_exists():
            table.drop_table()
        table.create_table()


def add_client(table_person: Person, table_phone: Phone, first_name, last_name, email, phones=None):
    person_id = table_person.insert(first_name, last_name, email)
    for phone in phones:
        add_phone(table_phone, person_id, phone)


def add_phone(table_phone: Phone, client_id, phone):
    table_phone.insert(phone, client_id)


def change_client(table_person: Person, table_phone: Phone, client_id, first_name=None, last_name=None, email=None, phones=None):
    table_person.update(client_id, first_name, last_name, email)
    if phones is not None:
        delete_phone(table_phone, client_id=client_id)
        for phone in phones:
            add_phone(table_phone, client_id, phone)


def delete_phone(table_phone: Phone, client_id, phone):
    table_phone.delete_by_fields(client_id, phone)


def delete_client(table_person: Person, client_id):
    table_person.delete(client_id)


def find_client(table_person: Person, first_name=None, last_name=None, email=None, phone=None):
    return table_person.search(first_name, last_name, email, phone)


if __name__ == '__main__':
    load_dotenv()
    db_host = getenv('DB_HOST')
    db_port = getenv('DB_PORT')
    db_user = getenv('DB_USER')
    db_pass = getenv('DB_PASS')

    with psycopg2.connect(dbname='postgres', user=db_user, password=db_pass, host=db_host, port=db_port) as conn:
        pers = Person(conn)
        numb = Phone(conn)

        # Создание БД
        create_db([pers, numb])

        # Заполняем БД данными


    conn.close()