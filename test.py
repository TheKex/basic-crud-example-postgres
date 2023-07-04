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

        conn = psycopg2.connect(dbname='postgres', user=db_user, password=db_pass, host=db_host, port=db_port)
        # пытаемся подключиться к базе данных
        values = {
            "a": 1,
            "b": 2
        }
        query = sql.SQL(",\n").join([
            sql.SQL("{field} = {value}").format(
                field=sql.Identifier(key),
                value=sql.Placeholder()
            ) for key in values.keys()
        ])

        print(query.as_string(conn))
        conn.close()
    except Exception as ex:
        # в случае сбоя подключения будет выведено сообщение в STDOUT
        raise ex