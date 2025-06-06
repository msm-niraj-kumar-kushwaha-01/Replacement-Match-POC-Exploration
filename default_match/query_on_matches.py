import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from postgres_db.matches.partnumbers_getter import get_connection
def get_matching_partnumber_details(part_number):
    query = """
    WITH matched_row AS (
        SELECT data
        FROM partnumber_matches_graph_table
        WHERE EXISTS (
            SELECT 1
            FROM jsonb_array_elements(data) elem
            WHERE elem->>'key' = %s
        )
    ),
    matched_keys AS (
        SELECT elem->>'key' AS partnumber
        FROM matched_row,
        LATERAL jsonb_array_elements(data) elem
        WHERE elem->>'key' <> %s
    )
    SELECT *
    FROM partnumber_input_table
    WHERE partnumber IN (SELECT partnumber FROM matched_keys);
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, (part_number, part_number))
    rows = cur.fetchall()
    
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    return [dict(zip(colnames, row)) for row in rows]

if __name__ == "__main__":
    part_number_to_search = "B"
    result = get_matching_partnumber_details(part_number_to_search)
    for row in result:
        print(row)