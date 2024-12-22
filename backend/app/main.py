from fastapi import FastAPI, HTTPException, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
import os
import shutil
from app.llm_service import LLMService

# Initialize FastAPI app
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
API_KEY = os.getenv("OPENAI_API_KEY")
DB_URL = os.getenv("DATABASE_URL")
DATASETS_FOLDER = "/app/Datasets"
os.makedirs(DATASETS_FOLDER, exist_ok=True)

# Initialize LLM service
llm_service = LLMService(api_key=API_KEY, db_url=DB_URL)

@app.post("/upload/")
async def upload_file(file: UploadFile):
    """
    Upload a dataset file, save it to the Datasets folder, and store it in the database.
    """
    try:
        # Save the uploaded file to the Datasets folder
        file_path = os.path.join(DATASETS_FOLDER, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Load the dataset into the database
        data = llm_service.load_dataset(file_path)
        llm_service.store_data_in_sql(data, table_name="car_sales_data")

        return {
            "message": f"File '{file.filename}' uploaded and stored in the database successfully.",
            "file_path": file_path,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File upload and database storage failed: {str(e)}")


@app.post("/etl/")
async def etl_endpoint(prompt: str = Form(...)):
    """
    Process user prompt, generate SQL query via LLM, execute it, and return the results.
    """
    try:
        # Generate SQL query from LLM
        refined_prompt = (
            "You are a helpful assistant that generates valid SQL queries. "
            "Use the 'car_sales_data' table in datasets database with columns: Date, Salesperson, Customer Name, Car Make, "
            "Car Model, Car Year, Sale Price, Commission Rate, Commission Earned. "
            "No other tables exist. You are only to use the car_sales_data table."
            "User query: "
        ) + prompt
        generated_sql = llm_service.generate_sql_query(refined_prompt)
        print("Generated SQL Query: ", generated_sql)

        # Execute the SQL query
        results = llm_service.execute_query(text(generated_sql))

        return {
            "message": "Query executed successfully.",
            "generated_sql": generated_sql,
            "result": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ETL process failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)