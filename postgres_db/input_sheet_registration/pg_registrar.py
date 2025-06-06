import json
import psycopg2
from psycopg2.extras import execute_values
from pg_schema_converter import parse_PartNumber
from Input_sheet_converter import process_files

# Your DB connection config
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "myuser",
    "password": "mypassword"
}

def enriched_process_files(input_folder):
    for item in process_files(input_folder):
        parsed = parse_PartNumber(item['PartNumber'])
        static_parts = parsed.get('static_parts')
        static_part = static_parts[0] if static_parts else ''

        yield (
            parsed.get('PartNumber'),
            item.get('Category', ''),
            item.get('Brand', ''),
            static_part,
            parsed.get('regex'),
            json.dumps(parsed.get('ranges')),         # Serialize ranges JSON
            item.get('PartNumber_Type_input', ''),
            item.get('Env_value', ''),
            json.dumps(item.get('Specs', {})),        # Serialize specs JSON
            json.dumps(item.get('mapper', {}))        # Serialize mapper JSON
        )

def batch_insert_from_generator(data_generator, batch_size=1000):
    """
    Inserts data from a generator into the PostgreSQL table in batches,
    skipping any duplicate (PartNumber, Brand, Category) entries.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        sql = """
        INSERT INTO partnumber_input_table 
        (partnumber, category, brand, static_part, regex_partnumber, ranges_partnumber, 
         partnumber_type_input, env_value, specs, mapper)
        VALUES %s
        ON CONFLICT (partnumber, brand, category) DO NOTHING
        """

        batch = []
        for row in data_generator:
            batch.append(row)
            if len(batch) == batch_size:
                execute_values(cur, sql, batch)
                conn.commit()
                print(f"Inserted batch of {batch_size} rows (duplicates skipped)")
                batch.clear()

        # Insert any remaining rows
        if batch:
            execute_values(cur, sql, batch)
            conn.commit()
            print(f"Inserted final batch of {len(batch)} rows (duplicates skipped)")

        cur.close()
    except Exception as e:
        print(f"Error during insert: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    input_folder_path = r"C:\Users\user\Downloads\test_folder"
    batch_insert_from_generator(enriched_process_files(input_folder_path), batch_size=1000)
