from os import getenv
from dotenv import load_dotenv

import psycopg2
from psycopg2 import sql
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
            conn.close()

        values = sql.SQL(', ').join(map(sql.Literal, [1, 2, 3]))


        conn.close()
    except Exception as ex:
        # в случае сбоя подключения будет выведено сообщение в STDOUT
        raise ex