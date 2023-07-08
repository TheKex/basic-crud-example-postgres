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
    table_phone.insert(phone_number=phone, person_id=client_id)


def change_client(table_person: Person, table_phone: Phone, client_id, first_name=None, last_name=None, email=None, phones=None):
    table_person.update(client_id, first_name, last_name, email)
    if phones is not None:
        delete_phone(table_phone, client_id=client_id)
        for phone in phones:
            add_phone(table_phone, client_id, phone)


def delete_phone(table_phone: Phone, client_id=None, phone=None):
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
    db_name = getenv('DB_NAME')

    with psycopg2.connect(dbname='postgres', user=db_user, password=db_pass, host=db_host, port=db_port) as conn:
        pers = Person(conn)
        numb = Phone(conn)

        # Создание БД
        create_db([pers, numb])

        # Заполняем БД данными
        persons = [
            ['Alex', 'Tomilin', 'some@mail.ru', ['+79223683636', '+79223652525']],
            ['Ivan', 'Tomilin', 'other@mail.ru', ['+79223655555', '+79223655555']],
            ['Vlad', 'Surin', 'surin@ya.ru',    ['+78000555353']],
            ['Andrey', 'Nofedov', 'andrey@google.com', []]
        ]
        for person in persons:
            add_client(pers, numb, *person)

        print("------------------------Все пользователи:--------------------------")
        all_persons = pers.search()
        print('\n'.join(str(person) for person in all_persons))

        print("\n------------------------Поиск пользователя :---------------------")
        person = find_client(pers, last_name='Tomilin', phone='+79223683636')
        print(person)

        person_phones = [row[1] for row in numb.search(person_id=person[0][0])]
        print(f"\nТелефоны пользователя {person[0][1]}: {person_phones}")

        print("Добавляем номер +880005553535")
        add_phone(numb, client_id=person[0][0], phone='+78005555555')

        person_phones = [row[1] for row in numb.search(person_id=person[0][0])]
        print(f"Телефоны пользователя {person[0][1]}: {person_phones}")

        print("\n------------------------Изменение пользователя :-----------------")
        change_client(pers, numb, person[0][0], last_name='Frolov', email='new@mail.com')
        person = find_client(pers, first_name='Alex', phone='+79223683636')
        print(person)
        person_phones = [row[1] for row in numb.search(person_id=person[0][0])]
        print(f"Телефоны пользователя {person[0][1]}: {person_phones}")

        print("\n------------------------Изменение пользователя с телефонами:-----")
        change_client(pers, numb, person[0][0], last_name='Petrov', email='old@mail.com', phones=['+79223683636'])
        person = find_client(pers, first_name='Alex')
        print(person)
        person_phones = [row[1] for row in numb.search(person_id=person[0][0])]
        print(f"Телефоны пользователя {person[0][1]}: {person_phones}")

        print("\n------------------------Удаляем телефоны:------------------------")
        delete_phone(numb, person[0][0], phone=person_phones[0])
        person_phones = [row[1] for row in numb.search(person_id=person[0][0])]
        print(f"Телефоны пользователя {person[0][1]}: {person_phones}")


        print("\n------------------------Удаляем пользователя Andrey:-------------")
        print("Все пользователи до:")
        all_persons = pers.search()
        print('\n'.join(str(person) for person in all_persons))

        person = find_client(pers, first_name='Andrey')
        delete_client(pers, person[0][0])

        print("Все пользователи после:")
        all_persons = pers.search()
        print('\n'.join(str(person) for person in all_persons))


    conn.close()
