from fastapi import FastAPI, Query
import uvicorn
app = FastAPI(
    title="PartNumber Registration API",
    description="""
This API accepts the path of a folder containing input files with part data.
When you POST the folder path:

- The files inside that folder will be processed and inserted into the Postgres database.
- The Weaviate vector database will be updated **only if the `weaviate_registrar.py` listener is running**, since it listens to Postgres triggers and syncs data accordingly.

Make sure to run `weaviate_registrar.py` separately if you want Weaviate to be kept in sync automatically.
""",
    version="1.0.0"
)

@app.post("/register-folder/", summary="Register parts from files in a folder")
async def register_folder(input_folder: str = Query(..., description="Absolute path to the input folder containing part files")):
    # Here you would call your processing function, e.g. enriched_process_files(input_folder)
    # and insert into Postgres.
    # The weaviate sync depends on the separate listener script running.
    return {"message": f"Started processing files in folder: {input_folder}"}

if __name__ == "__main__":
    uvicorn.run("pg_registration_api:app", host="127.0.0.1", port=8000, reload=True)
