from fastapi import FastAPI, HTTPException, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
import os
import shutil
import pandas as pd
from app.llm_service import LLMService
from fastapi.responses import FileResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_KEY = os.getenv("OPENAI_API_KEY")
DB_URL = os.getenv("DATABASE_URL")
DATASETS_FOLDER = "/app/Datasets"
METADATA_PATH = "/app/Datasets/dataset_metadata.json"
os.makedirs(DATASETS_FOLDER, exist_ok=True)

llm_service = LLMService(api_key=API_KEY, db_url=DB_URL, metadata_path=METADATA_PATH)

@app.post("/upload/")
async def upload_file(file: UploadFile):
    try:
        file_path = os.path.join(DATASETS_FOLDER, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        data = llm_service.load_dataset(file_path)
        llm_service.store_data_in_sql(data, table_name="car_sales_data")

        return {"message": f"File '{file.filename}' uploaded and stored in the database successfully.", "file_path": file_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File upload and database storage failed: {str(e)}")

@app.post("/etl/execute/")
async def etl_execute_endpoint(prompt: str = Form(...)):
    try:
        refined_prompt = llm_service.generate_dynamic_prompt(prompt)
        generated_sql = llm_service.generate_sql_query(refined_prompt)
        results = llm_service.execute_query(text(generated_sql))

        if not results:
            return {
                "message": "Query executed successfully but returned no data.",
                "generated_sql": generated_sql,
                "files": None
            }

        return {
            "message": "Query executed successfully.",
            "generated_sql": generated_sql,
            "results": results
        }

    except Exception as e:
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

        llm_service.store_data_in_sql(df, table_name=save_table_name)

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
async def download_file(file_path: str):
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(file_path)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
