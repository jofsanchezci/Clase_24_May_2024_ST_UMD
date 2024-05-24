import sqlite3

# Conexión a la base de datos (crea una nueva si no existe)
conn = sqlite3.connect('test.db')

# Crear un cursor para ejecutar comandos SQL
cursor = conn.cursor()

try:
    # Iniciar una transacción
    conn.execute('BEGIN TRANSACTION')
    
    # Crear una tabla de ejemplo
    cursor.execute('''CREATE TABLE IF NOT EXISTS usuarios
                      (id INTEGER PRIMARY KEY, nombre TEXT, edad INTEGER)''')

    # Insertar datos de ejemplo
    cursor.execute("INSERT INTO usuarios (nombre, edad) VALUES (?, ?)", ('Juan', 30))
    cursor.execute("INSERT INTO usuarios (nombre, edad) VALUES (?, ?)", ('María', 25))
    
    # Commit para aplicar los cambios
    conn.commit()
    print("Transacción exitosa. Cambios aplicados.")

except Exception as e:
    # Rollback en caso de error
    conn.rollback()
    print(f"Error durante la transacción: {e}. Se ha realizado rollback.")

finally:
    # Cerrar la conexión
    conn.close()
