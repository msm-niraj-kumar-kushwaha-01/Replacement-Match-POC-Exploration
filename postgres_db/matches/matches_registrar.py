import json
from postgres_db.matches.partnumbers_getter import get_connection

def insert_partnumber_data(data):
    """
    data: list of dicts, e.g.
    [
        {"key": "A", "value": 1},
        {"key": "B", "value": 2}
    ]
    """
    conn = get_connection()
    cur = conn.cursor()
    json_data = json.dumps(data)
    cur.execute(
        "INSERT INTO partnumber_matches_graph_table (data) VALUES (%s)",
        (json_data,)
    )
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    # Example insert
    insert_partnumber_data([
        {"key": "A", "value": 1},
        {"key": "B", "value": 2},
        {"key": "C", "value": 3},
        {"key": "D", "value": 4}
    ])
