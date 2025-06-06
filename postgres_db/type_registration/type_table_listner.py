import json
import select
import psycopg2
from matches.input_type_output_types_extracter import process_type_input_notification
from matches.match_maker import process_matches
PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "myuser",
    "password": "mypassword"
}


def main():
    pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cur = pg_conn.cursor()
    cur.execute("LISTEN type_input_events;")
    print("🔔 Listening on channel 'type_input_events'...")

    try:
        while True:
            if select.select([pg_conn], [], [], 5) == ([], [], []):
                continue

            pg_conn.poll()
            while pg_conn.notifies:
                notify = pg_conn.notifies.pop(0)
                try:
                    payload = json.loads(notify.payload)
                    input_type, output_type, specs_to_be_matched = process_type_input_notification(payload)
                    process_matches(input_type, output_type, specs_to_be_matched)
                except json.JSONDecodeError:
                    print(f"[❌] Invalid JSON payload: {notify.payload}")

    except KeyboardInterrupt:
        print("\n👋 Exiting on keyboard interrupt.")
    finally:
        cur.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
