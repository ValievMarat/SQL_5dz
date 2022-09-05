import psycopg2
from pprint import pprint

def create_structure_db(conn):
    with conn.cursor() as cursor:
        # удаление таблиц
        cursor.execute("""
        DROP TABLE IF EXISTS phones;
        DROP TABLE IF EXISTS clients;
        """)

        # создание таблиц
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            surname VARCHAR(200) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES clients(id),
            phone VARCHAR(100) UNIQUE NOT NULL
        );
        """)

        conn.commit()  # фиксируем в БД


def add_new_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cursor:

        cursor.execute("""
        INSERT INTO clients(name, surname, email)
            VALUES(%s,%s,%s) RETURNING id;
        """, (first_name, last_name, email))
        client_id = cursor.fetchone()[0]

    if isinstance(phones, list):
        for phone in phones:
            add_new_phone_by_client(conn, client_id, phone)


def add_new_phone_by_client(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO phones(client_id, phone)
            VALUES(%s,%s) RETURNING id;
        """, (client_id, phone))
        conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cursor:
        if first_name is not None:
            cursor.execute("""
            UPDATE clients
                SET name=%s
                WHERE id=%s;
            """, (first_name, client_id))

        if last_name is not None:
            cursor.execute("""
            UPDATE clients
                SET surname=%s
                WHERE id=%s;
            """, (last_name, client_id))

        if email is not None:
            cursor.execute("""
            UPDATE clients
                SET email=%s
                WHERE id=%s;
            """, (email, client_id))

        conn.commit()

        if isinstance(phones, list):
            # полностью удаляем все существующие телефоны клиента и добавляем по новой
            cursor.execute("""
            DELETE FROM phones
            WHERE client_id=%s;
            """, (client_id,))
            conn.commit()
            for phone in phones:
                add_new_phone_by_client(conn, client_id, phone)


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute("""
        DELETE FROM phones
            WHERE client_id=%s AND phone=%s;
        """, (client_id,phone))
        conn.commit()


def delete_client(conn, client_id):
    # удаляем и телефоны, и клиента
    with conn.cursor() as cursor:
        cursor.execute("""
        DELETE FROM phones
            WHERE client_id=%s;
            """, (client_id,))

        cursor.execute("""
        DELETE FROM clients
            WHERE id=%s;
        """, (client_id,))
        conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT * FROM clients c
        LEFT JOIN phones p
            ON c.id = p.client_id
            WHERE (c.name=%s OR %s IS NULL)
                AND (c.surname=%s OR %s IS NULL)
                AND (c.email=%s OR %s IS NULL)
                AND (p.phone=%s OR %s IS NULL);
        """, (first_name, first_name, last_name, last_name, email, email, phone, phone))
        print()
        pprint(cursor.fetchall())


if __name__ == '__main__':

    database_name = 'clients'
    user = 'postgres'
    password = '90210'

    with psycopg2.connect(database=database_name, user=user, password=password) as conn:
        create_structure_db(conn)

        add_new_client(conn, 'Ivan', 'Ivanov', 'ivan@mail.ru')
        add_new_client(conn, 'Ivan', 'Petrov', 'petr@mail.ru', ['211-46-31', '322-25-21'])

        add_new_phone_by_client(conn, 1, '344-12-12')
        add_new_phone_by_client(conn, 2, '8(912)211-12-12')

        # меняем только имя
        change_client(conn, 1, 'Dmitry')

        # меняем фамилию и почту
        change_client(conn, 1, None, 'Vazov', 'dvazov@mail.ru')

        #меняем телефоны и почту
        change_client(conn, 1, None, None, 'newmail@mail.ru', ['777-77-77', '555-55-55'])

        delete_phone(conn, 1, '555-55-55')

        delete_client(conn, 1)

        add_new_client(conn, 'Ivan', 'Ivanov', 'ivan@mail.ru', ['44-44-44'])
        add_new_client(conn, 'Ivan', 'Perunov', 'perunov@mail.ru', ['22-22-12'])

        # поиск всех Иванов
        find_client(conn, 'Ivan')

        #поиск по номеру телефона
        find_client(conn, None, None, None, '211-46-31')

