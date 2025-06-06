import psycopg2
from typing import List, Dict
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "myuser",
    "password": "mypassword"
}

def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )

def get_partnumbers_by_type_input(type_input: str, batch_size=1000) -> List[Dict]:
    results = []
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(name='server_side_cursor') as cur:
            cur.itersize = batch_size  # fetch batch_size rows at a time
            cur.execute("SELECT * FROM get_partnumbers_by_type_input(%s);", (type_input,))
            for row in cur:
                results.append({
                    "partnumber": row[0],
                    "specs": row[1]
                })
    return results
