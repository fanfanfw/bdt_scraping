# mudahmy_service_playwright/database.py
from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

def get_connection():
    try:
        print(f"DB_NAME: {os.getenv('DB_NAME')}")
        print(f"DB_USER: {os.getenv('DB_USER')}")
        print(f"DB_PASSWORD: {os.getenv('DB_PASSWORD')}")
        print(f"DB_HOST: {os.getenv('DB_HOST')}")
        print(f"DB_PORT: {os.getenv('DB_PORT')}")
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        print("✅ Koneksi ke database berhasil dan valid.")
        return conn
    except Exception as e:
        print(f"❌ Error koneksi ke database: {e}")
        raise e