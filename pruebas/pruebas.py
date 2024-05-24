import unittest
import sqlite3

# Función de utilidad para imprimir el estado de la transacción
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

# Función para crear la tabla
def create_table(conn):
    create_table_query = '''CREATE TABLE IF NOT EXISTS users
                            (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'''
    execute_transaction(conn, create_table_query)

class TestDatabaseTransactions(unittest.TestCase):
    def setUp(self):
        # Conectar a la base de datos (en memoria para pruebas)
        self.conn = sqlite3.connect(':memory:')
        create_table(self.conn)

    def tearDown(self):
        # Cerrar la conexión
        self.conn.close()

    def test_commit_transaction(self):
        # Caso exitoso: Inserción de datos y commit
        insert_query = "INSERT INTO users (name, age) VALUES (?, ?)"
        insert_data = ('John', 30)
        execute_transaction(self.conn, insert_query, insert_data)
        self.conn.commit()

        # Verificar que los datos se han insertado
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE name=?", (insert_data[0],))
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[1], 'John')
        self.assertEqual(result[2], 30)

    def test_rollback_transaction(self):
        # Caso con error: Inserción de datos y rollback
        insert_query = "INSERT INTO users (name, age) VALUES (?, ?)"
        execute_transaction(self.conn, insert_query, ('Alice', 25))

        # Provocar un error
        with self.assertRaises(sqlite3.OperationalError):
            execute_transaction(self.conn, "INSERT INTO users (name, age) VALUES (?, ?)", (None, 'invalid_age'))
        
        # Realizar rollback
        self.conn.rollback()

        # Verificar que los datos no se han insertado
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE name=?", ('Alice',))
        result = cursor.fetchone()
        self.assertIsNone(result)

    def test_partial_commit_rollback(self):
        # Caso con commit parcial y rollback
        insert_query = "INSERT INTO users (name, age) VALUES (?, ?)"
        execute_transaction(self.conn, insert_query, ('Bob', 35))
        self.conn.commit()  # Commit parcial

        execute_transaction(self.conn, insert_query, ('Charlie', 40))

        # Provocar un error
        with self.assertRaises(sqlite3.OperationalError):
            execute_transaction(self.conn, "INSERT INTO users (name, age) VALUES (?, ?)", ('Diana', 'invalid_age'))
        
        # Realizar rollback
        self.conn.rollback()

        # Verificar que solo los datos antes del error se han insertado
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users")
        results = cursor.fetchall()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][1], 'Bob')
        self.assertEqual(results[0][2], 35)

if __name__ == '__main__':
    unittest.main()
