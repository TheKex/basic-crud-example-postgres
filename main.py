from os import getenv
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import NamedTupleCursor

from db_tools import Person, Phone

if __name__ == '__main__':
    load_dotenv()
    db_host = getenv('DB_HOST')
    db_port = getenv('DB_PORT')
    db_user = getenv('DB_USER')
    db_pass = getenv('DB_PASS')

    print('postgres', db_user, db_pass, db_host, sep=' *** ')
    try:
        # пытаемся подключиться к базе данных
        conn = psycopg2.connect(dbname='postgres', user=db_user, password=db_pass, host=db_host, port=db_port)

        try:
            with conn.cursor(cursor_factory=NamedTupleCursor) as curs:
                curs.execute('SELECT * FROM public.employee;')
                all_users = curs.fetchall()
                print(all_users)
        except:
            print('Что-то пошло не так')

        pers = Person(conn, False)
        numb = Phone(conn, False)
        # pers.drop_table()
        # # numb.drop_table()
        # pers.create_table()
        # numb.create_table()
        print(pers.connection)
        user_id = pers.insert('Alex', 'Tomilin', 'al.tomilin@mail.ru')

        print(f"user_id = {user_id}")

        del_id = pers.delete(user_id)
        print(f"del_id = {del_id}")

        upd_id = pers.update(1, first_name='Alexey')
        print(f"upd_id = {upd_id}")


    except Exception as ex:
        # в случае сбоя подключения будет выведено сообщение в STDOUT
        raise ex
