import sqlite3

# Función para imprimir el estado de la transacción
def print_transaction_status(conn):
    status = conn.in_transaction if conn else False
    print(f"Estado de la transacción: {'En transacción' if status else 'Fuera de la transacción'}")

# Función para ejecutar una transacción
def execute_transaction(conn, query, parameters=None):
    try:
        if not conn.in_transaction:
            conn.execute('BEGIN TRANSACTION')

        if parameters:
            conn.execute(query, parameters)
        else:
            conn.execute(query)

        print("Operación ejecutada con éxito")

    except Exception as e:
        print(f"Error durante la transacción: {e}")
        conn.rollback()

# Conexión a la base de datos
conn = sqlite3.connect('test.db')

try:
    # Crear tabla de usuarios
    create_table_query = '''CREATE TABLE IF NOT EXISTS users
                            (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'''
    execute_transaction(conn, create_table_query)

    # Insertar datos en la tabla
    insert_query = "INSERT INTO users (name, age) VALUES (?, ?)"
    insert_data = [('John', 30), ('Alice', 25), ('Bob', 35)]

    for data in insert_data:
        execute_transaction(conn, insert_query, data)

    # Commit de la transacción
    conn.commit()
    print("Transacción completada con éxito")

except Exception as e:
    print(f"Error general: {e}")
    conn.rollback()

finally:
    # Imprimir el estado final de la transacción y cerrar la conexión
    print_transaction_status(conn)
    conn.close()
