import psycopg2
import pandas as pd
import json

PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "myuser",
    "password": "mypassword"
}

def generate_pairs(lst):
    for i in lst:
        for j in lst:
            if i != j:
                yield (i, j)

conn = psycopg2.connect(**PG_CONN_PARAMS)
cur = conn.cursor()

df = pd.read_excel('type_input.xlsx')

for idx, row in df.iterrows():
    type_list = [t.strip() for t in row['similar_types'].split(',') if t.strip()]
    specs = row['specs_to_be_matched']

    # Insert all pairs silently
    for type_input, type_output in generate_pairs(type_list):
        cur.execute(
            """
            INSERT INTO type_input_table (type_input, type_output, Specs_to_be_Matched)
            VALUES (%s, %s, %s)
            ON CONFLICT (type_input, type_output) DO NOTHING
            """,
            (type_input, type_output, specs)
        )

    # Send ONE notification per Excel row
    payload = {
        'action': 'batch_insert',
        'similar_types': row['similar_types'],  
        'specs_to_be_matched': specs
    }
    cur.execute("SELECT pg_notify('type_input_events', %s)", (json.dumps(payload),))

conn.commit()
cur.close()
conn.close()

print("Data inserted and single notifications sent per Excel row.")
