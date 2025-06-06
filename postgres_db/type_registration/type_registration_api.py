from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import psycopg2
import pandas as pd

PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "myuser",
    "password": "mypassword"
}

app = FastAPI()

class FolderPath(BaseModel):
    folder_path: str


def generate_pairs(lst):
    for i in lst:
        for j in lst:
            if i != j:
                yield (i, j)


def process_type_input_excel(folder_path: str):
    # Validate folder and file
    if not os.path.isdir(folder_path):
        raise FileNotFoundError("Folder path does not exist")

    file_path = os.path.join(folder_path, "type_input.xlsx")

    if not os.path.isfile(file_path):
        raise FileNotFoundError("type_input.xlsx not found in folder")

    # Read Excel file
    df = pd.read_excel(file_path)

    # Connect to DB
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()

    try:
        for idx, row in df.iterrows():
            type_list = [t.strip() for t in str(row['similar_types']).split(',') if t.strip()]
            specs = row['specs_to_be_matched']

            for type_input, type_output in generate_pairs(type_list):
                cur.execute(
                    """
                    INSERT INTO type_input_table (type_input, type_output, Specs_to_be_Matched)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (type_input, type_output) DO NOTHING
                    """,
                    (type_input, type_output, specs)
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


@app.post("/upload_excel/")
def upload_excel(data: FolderPath):
    try:
        process_type_input_excel(data.folder_path)
        return {"message": "Data inserted successfully."}
    except FileNotFoundError as fnf_err:
        raise HTTPException(status_code=400, detail=str(fnf_err))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database or processing error: {exc}")
