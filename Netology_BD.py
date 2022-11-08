import psycopg2
from psycopg2 import sql

#Функция, создающая структуру БД (таблицы)
def create_db(conn):
    cur = conn.cursor()
    cur.execute("""       
            CREATE TABLE IF NOT EXISTS customer(
            customer_id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            surname VARCHAR(60) NOT NULL,
            email VARCHAR(70) NOT NULL UNIQUE);
            """)

    cur.execute("""         
            CREATE TABLE IF NOT EXISTS phones(
            phone_id SERIAL PRIMARY KEY,
            num_phone VARCHAR(16) UNIQUE,
            customer_id INT not null references customer(customer_id) ON DELETE CASCADE);
            """)
    cur.close()
    conn.commit()
    return f'Tables "Customer" and "Phones" has cteate'

#Функция, позволяющая добавить нового клиента
def add_client(conn, first_name: str, last_name: str, email: str, phone = None) -> str:
    cur = conn.cursor()
    if phone == None:
        SQL = "INSERT INTO customer(name, surname, email) VALUES (%s, %s, %s);"
        data = (first_name, last_name, email)
        cur.execute(SQL, data)
        cur.close()
        conn.commit()
        return f'New customer {last_name} {first_name} cteated'

    else:
        SQL_1 = "INSERT INTO customer(name, surname, email) VALUES (%s, %s, %s);"
        data_1 = (first_name, last_name, email)
        cur.execute("""INSERT INTO customer(name, surname, email) VALUES (%s, %s, %s) RETURNING customer_id;""",
                    (first_name, last_name, email))
        id = cur.fetchone()
        SQL_2 = "INSERT INTO phones(num_phone, customer_id) VALUES(%s, %s)"
        data_2 = (phone, id[0])
        cur.execute(SQL_2, data_2)
        cur.close()
        conn.commit()
        return f'New customer {last_name} {first_name} cteated'

#Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, client_id: int, phone: str) -> str:
    cur = conn.cursor()
    SQL = "INSERT INTO phones(num_phone, customer_id) VALUES(%s, %s)"
    data = (phone, client_id)
    cur.execute(SQL, data)
    cur.close()
    conn.commit()
    return f'Number phone {phone} added'

#Функция, позволяющая изменить данные о клиенте
def change_client(conn, client_id: int, first_name=None, last_name=None, email=None, phones=None) -> str:
    cur = conn.cursor()
    if first_name != None:
        cur.execute(
            sql.SQL("UPDATE {} SET name = %s WHERE customer_id=%s").format(sql.Identifier('customer')),
            (first_name, client_id))
        print(f'Name has change')

    if last_name != None:
        cur.execute(
            sql.SQL("UPDATE {} SET surname = %s WHERE customer_id=%s").format(Identifier('customer')),
            (last_name, client_id))
        print(f'Surname has change')
    if email != None:
        cur.execute(
            sql.SQL("UPDATE {} SET email = %s WHERE customer_id=%s").format(sql.Identifier('customer')),
            (email, client_id))
        print(f'E-mail has change')
    if phones != None and len(phones) == 1:
        SQL = "UPDATE INTO phones(num_phone) VALUES(%s) WHERE client_id=%s"
        data = (phones[0], client_id)
        cur.execute(SQL, data)
        print(f'Number phone {phone} also update')
    if phones != None and len(phones) > 1:
        for i in range(len(phones)):
            SQL = "UPDATE INTO phones(num_phone) VALUES(%s) WHERE client_id=%s"
            data = (phones[i], client_id)
            cur.execute(SQL, data)
        print(f'Number phone {phone} also update')
    cur.close()
    conn.commit()
    return

#Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, client_id: int, phone: str) -> str:
    cur = conn.cursor()
    cur.execute("""DELETE FROM phones WHERE num_phone=%s AND customer_id=%s""",(phone, client_id))
    cur.close()
    conn.commit()
    return f'Number phone {phone} has delete'

#Функция, позволяющая удалить существующего клиента
def delete_client(conn, client_id: int) -> str:
    cur = conn.cursor()
    SQL = "DELETE FROM customer WHERE customer_id = %s"
    data = (client_id,)
    cur.execute(SQL, data)
    cur.close()
    conn.commit()
    print(f'Customer has delete')
    return

#Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
def find_client(conn, first_name=None, last_name=None, email=None, phone=None) ->str:
    cur = conn.cursor()
    if email != None:
        cur.execute("""SELECT name, surname, email, num_phone
                FROM customer c LEFT JOIN  phones p ON c.customer_id = p.customer_id 
                WHERE email=%s;""", (email,))
        print(cur.fetchall())
        cur.close()
        conn.commit()
        return
    if phone != None:
        cur.execute("""SELECT name, surname, email, num_phone
                FROM phones INNER JOIN customer ON phones.customer_id = customer.customer_id
                WHERE num_phone=%s;""", (phone,))
        print(cur.fetchall())
        cur.close()
        conn.commit()
        return
    if first_name != None and last_name !=None:
        cur.execute("""SELECT name, surname, email, num_phone
                        FROM customer LEFT JOIN phones ON phones.customer_id = customer.customer_id
                        WHERE name=%s and surname=%s;""", (first_name, last_name))
        print(cur.fetchall())
        cur.close()
        conn.commit()
        return


with psycopg2.connect(database="Work_with_PSQL_from_Python", user="postgres", password="Zrhfcfdxbr") as conn:
    print(create_db(conn))
    print(add_client(conn, "Мария", "Вдовина", "maria@mail.ru", 89656186255))
    print(add_client(conn, "Дмитрий", "Вдовин", "9274547212@mail.ru"))
    print(add_phone(conn, 2, '9274570416'))
    print(delete_phone(conn, 2, "9274570416"))
    find_client(conn, 'Дмитрий', 'Вдовин')
    find_client(conn, email='9274547212@mail.ru')
    find_client(conn, phone='9274570416')
    change_client(conn, 1, first_name="Дмитрий")
    change_client(conn, 1, email="Дмитрий")
    print(delete_client(conn, 1))

conn.close()