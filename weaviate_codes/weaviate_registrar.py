# pg_to_weaviate_sync.py

import json
import select
import time
import uuid
import re

import psycopg2
from psycopg2.extras import RealDictCursor
import weaviate
from range_converter import specs_range_modifier_for_embedding

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────

PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "myuser",
    "password": "mypassword"
}

WEAVIATE_URL = "http://localhost:8080"
WEAVIATE_CLASS = "PartNumber"


# ─── UTILITY FUNCTIONS ─────────────────────────────────────────────────────────

def generate_uuid(brand: str, category: str, part_no: str) -> str:
    """
    Generate a deterministic UUID from brand + category + part number
    so we can sync easily later.
    """
    base = f"{brand}__{category}__{part_no}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))




# ─── SYNC LOGIC ────────────────────────────────────────────────────────────────

def process_notification(payload: dict, pg_conn, wv_client):
    """
    Handle INSERT/UPDATE/DELETE messages.
    """
    collection = wv_client.collections.get(WEAVIATE_CLASS)

    action = payload.get("action")
    part_no = payload.get("PartNumber")
    brand = payload.get("Brand")
    category = payload.get("Category")
    print(action,part_no,brand,category)
    uuid_key = generate_uuid(brand, category, part_no)

    if action in ("INSERT", "UPDATE"):
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    "partnumber",
                    "brand",
                    "category",
                    "partnumber_type_input" AS type_input,
                    "env_value" AS env_value,
                    "specs",
                    "mapper"
                FROM partnumber_input_table
                WHERE "partnumber" = %s AND "brand" = %s AND "category" = %s
                """,
                (part_no, brand, category)
            )
            row = cur.fetchone()
            if not row:
                print(f"[WARN] No row found in Postgres for {part_no}, {brand}, {category}")
                return

        specs_dict = row["specs"]
        modified_specs = specs_range_modifier_for_embedding(specs_dict)

        props = {
            "PartNumber": row["partnumber"],
            "Brand": row["brand"],
            "Category": row["category"],
            "type_input": row["type_input"],
            "env_value": row["env_value"],
            "specs": json.dumps(specs_dict),
            "modified_specs": modified_specs,
            "mapper": json.dumps(row["mapper"])
        }

        try:
            collection.data.insert(properties=props, uuid=uuid_key)
            print(f"[✅] Upserted {uuid_key} into Weaviate.")
        except Exception as e:
            print(f"[❌] Failed to insert/update {uuid_key}: {e}")

    elif action == "DELETE":
        try:
            collection.data.delete_by_id(uuid_key)
            print(f"[🗑️] Deleted {uuid_key} from Weaviate.")
        except weaviate.exceptions.NotFoundException:
            print(f"[⚠️] Tried to delete {uuid_key} but it was not found.")
        except Exception as e:
            print(f"[❌] Failed to delete {uuid_key}: {e}")
    else:
        print(f"[⚠️] Unknown action '{action}' received: {payload}")


# ─── MAIN RUNNER ───────────────────────────────────────────────────────────────

def main():
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cur = pg_conn.cursor()
    cur.execute("LISTEN part_events;")
    print("🔔 Listening on channel 'part_events'...")

    # Connect to Weaviate
    wv_client = weaviate.connect_to_local()

    try:
        while True:
            if select.select([pg_conn], [], [], 5) == ([], [], []):
                continue

            pg_conn.poll()
            while pg_conn.notifies:
                notify = pg_conn.notifies.pop(0)
                try:
                    payload = json.loads(notify.payload)
                    process_notification(payload, pg_conn, wv_client)
                except json.JSONDecodeError:
                    print(f"[❌] Invalid JSON payload: {notify.payload}")
    except KeyboardInterrupt:
        print("\n👋 Exiting on keyboard interrupt.")
    finally:
        cur.close()
        pg_conn.close()
        wv_client.close()


if __name__ == "__main__":
    main()
