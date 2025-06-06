import psycopg2
import psycopg2.extras

# Connect to your database
conn = psycopg2.connect(
    dbname="your_db",
    user="your_user",
    password="your_password",
    host="localhost",
    port="5432"
)

# Enable returning rows as dictionaries
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Execute the function call
type_input_value = 'capacitor'
cur.execute("SELECT * FROM get_partnumbers_by_type_input(%s);", (type_input_value,))

# Fetch all results
results = cur.fetchall()

# Convert to list of dictionaries (optional, if you used DictCursor it's already dict-like)
results_list = [dict(row) for row in results]

# Example output
for row in results_list:
    print(row)

cur.close()
conn.close()
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
