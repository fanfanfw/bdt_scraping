import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_database_connection():
    """Fungsi untuk menghubungkan ke database PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except Exception as e:
        print(f"❌ Error koneksi database: {e}")
        return None
