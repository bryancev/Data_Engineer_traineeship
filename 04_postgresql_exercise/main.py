import psycopg2

# PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",
    port=5433,
    user="user",
    password="password",
    dbname="example_db"
)
pg_cursor = pg_conn.cursor()
pg_cursor.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT);")
pg_cursor.execute("INSERT INTO users(name) VALUES (%s)", ("Alice",))
pg_cursor.execute("SELECT * FROM users;")
print("PostgreSQL:", pg_cursor.fetchall())
pg_conn.commit()
pg_cursor.close()
pg_conn.close()
