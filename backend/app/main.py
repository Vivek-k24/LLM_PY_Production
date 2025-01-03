from fastapi import FastAPI, HTTPException, Form, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
import os
import shutil
import pandas as pd
from app.llm_service import LLMService
from fastapi.responses import FileResponse, JSONResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Environment variables
API_KEY = os.getenv("OPENAI_API_KEY")
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123456@localhost:5432/app_db")
DATASETS_FOLDER = os.path.join(BASE_DIR, "../Datasets")
METADATA_PATH = os.path.join(DATASETS_FOLDER, "dataset_metadata.json")

# Ensure datasets folder exists
os.makedirs(DATASETS_FOLDER, exist_ok=True)

llm_service = LLMService(api_key=API_KEY, db_url=DB_URL, metadata_path=METADATA_PATH)

@app.post("/upload/")
async def upload_file(file: UploadFile, purpose: str = Form(...), replace : bool = Form(False)):
    """
    Upload a dataset, store it in the database, and update dataset_metadata.json.
    """
    try:
        file_path = os.path.join(DATASETS_FOLDER, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Load and preprocess the dataset
        data, date_columns, time_columns, timestamp_columns = llm_service.load_dataset(file_path)

        # Define dataset name
        dataset_name = os.path.splitext(file.filename)[0]

        # Determine time_filter_column and date_filter_column
        time_filter_column = timestamp_columns[0] if timestamp_columns else (time_columns[0] if time_columns else None)
        date_filter_column = date_columns if date_columns else "No date columns detected"

        # Generate dynamic descriptions for columns using LLM
        column_descriptions = {}
        for col in data.columns:
            column_descriptions[col] = llm_service.generate_column_description(col)

        # Update metadata
        llm_service.metadata_manager.add_dataset_metadata(
            dataset_name,
            column_descriptions,
            purpose=purpose,
            time_filter_column = time_filter_column,
            date_filter_column = date_filter_column,
            replace=replace
        )
        llm_service.metadata_manager.save_metadata()

        # Save the preprocessed dataset to the database
        data.to_sql(dataset_name, con=llm_service.engine, if_exists="replace", index=False)

        # Clean up the temporary file
        os.remove(file_path)

        return {
            "message": f"File '{file.filename}' uploaded, processed, and stored in the database. Metadata updated.",
            "dataset_name": dataset_name,
            "columns": column_descriptions,
            "time_filter_column": time_filter_column or "No time column detected",
            "date_filter_column": date_filter_column or "No date columns detected"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File upload and metadata update failed: {str(e)}")


@app.post("/etl/execute/")
async def etl_execute_endpoint(dataset_name: str = Form(...),prompt: str = Form(...)):
    try:
        metadata = llm_service.metadata_manager.get_metadata()
        if dataset_name not in metadata:
            raise ValueError(f"Dataset '{dataset_name}' not found in metadata.")
        
        dataset_metadata = metadata[dataset_name]
        print(f"Debug: Dataset metadata: {dataset_metadata}")
        
        #Generate SQL query using LLM and execute
        refined_prompt = llm_service.generate_dynamic_prompt(prompt, dataset_name)
        generated_sql = llm_service.generate_sql_query(refined_prompt, dataset_name)
        results = llm_service.execute_query(text(generated_sql))
        
        if not results:
            response = {
                "message": "Query executed successfully but returned no data.",
                "generated_sql": generated_sql,
                "files": None
            }
            return response

        response = {
            "message": "Query executed successfully.",
            "generated_sql": generated_sql,
            "results": results
        }
        return response

    except Exception as e:
        print(f"DEBUG: Exception: {str(e)}")
        raise HTTPException(status_code=500, detail=f"ETL execution failed: {str(e)}")

@app.post("/etl/save/")
async def etl_save_endpoint(prompt: str = Form(...), save_table_name: str = Form(...)):
    try:
        refined_prompt = llm_service.generate_dynamic_prompt(prompt)
        generated_sql = llm_service.generate_sql_query(refined_prompt)
        results = llm_service.execute_query(text(generated_sql))

        if not results:
            return {
                "message": "Query executed successfully but returned no data.",
                "generated_sql": generated_sql,
                "saved_table_name": save_table_name,
                "files": None
            }

        df = pd.DataFrame(results)
        df.columns = [f"column_{i}" if not col or col.isspace() else col for i, col in enumerate(df.columns)]

        # Save the DataFrame directly to the database
        llm_service.store_dataframe_in_sql(df, table_name=save_table_name)

        output_folder = os.path.join(DATASETS_FOLDER, "output")
        os.makedirs(output_folder, exist_ok=True)
        csv_path = os.path.join(output_folder, f"{save_table_name}.csv")
        xlsx_path = os.path.join(output_folder, f"{save_table_name}.xlsx")
        pdf_path = os.path.join(output_folder, f"{save_table_name}.pdf")

        df.to_csv(csv_path, index=False)
        df.to_excel(xlsx_path, index=False)

        return {
            "message": "ETL process completed successfully.",
            "generated_sql": generated_sql,
            "saved_table_name": save_table_name,
            "files": {
                "csv": csv_path,
                "xlsx": xlsx_path,
                "pdf": pdf_path
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ETL process failed: {str(e)}")


@app.get("/download/")
async def download_table(table_name: str = Query(..., description="The name of the table to download"),
                         file_format: str = Query("csv", description="The desired file format: 'csv' or 'xlsx'")):
    """
    Download a table from the database as a .csv or .xlsx file.
    - table_name: The name of the table to download.
    - file_format: The desired file format ('csv' or 'xlsx').
    """
    try:
        # Validate file format
        if file_format not in ["csv", "xlsx"]:
            raise HTTPException(status_code=400, detail="Invalid file format. Use 'csv' or 'xlsx'.")

        # Query to fetch data from the database
        query = f"SELECT * FROM {table_name}"
        with llm_service.engine.connect() as connection:
            df = pd.read_sql(query, connection)

        if df.empty:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' is empty or does not exist.")

        # Path to save the file
        output_folder = os.path.join(DATASETS_FOLDER, "output")
        os.makedirs(output_folder, exist_ok=True)
        file_path = os.path.join(output_folder, f"{table_name}.{file_format}")

        # Save the file in the requested format
        if file_format == "csv":
            df.to_csv(file_path, index=False)
        elif file_format == "xlsx":
            df.to_excel(file_path, index=False)

        # Add Content-Disposition header to set the filename
        headers = {
            "Content-Disposition": f'attachment; filename="{table_name}.{file_format}"'
        }

        # Return the file as a response with the specified filename
        return FileResponse(file_path, headers=headers)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
