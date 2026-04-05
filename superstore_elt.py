import psycopg2
from sqlalchemy import create_engine

conn = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="qweflx7123",
    host="localhost",
    port="5432"
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'superstore_elt'")
exists = cursor.fetchone()
if not exists:
    cursor.execute("CREATE DATABASE superstore_elt")
    print("Database 'superstore_elt' created")
else:
    print("Database 'superstore_elt' already exists")

cursor.close()
conn.close()

conn_string = "postgresql://postgres:your_password@localhost:5432/superstore_elt"
engine = create_engine(conn_string)
print("Connected to superstore_elt database")