from sqlalchemy.exc import SQLAlchemyError
from app.database import get_sql_engine, get_mongo_client
import pandas as pd
import pdfplumber
import json
import os
import logging
from fastapi import HTTPException


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def process_dataset(file):
    file_ext = file.filename.split('.')[-1].lower()
    temp_path = os.path.join(OUTPUT_DIR, file.filename)
    with open(temp_path, "wb") as f:
        f.write(file.file.read())

    if file_ext == 'csv':
        df = pd.read_csv(temp_path)
        table_name = file.filename.split('.')[0]
        engine = get_sql_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        return {"table_name": table_name, "row_count": len(df)}
    raise ValueError("Unsupported file type.")


async def store_in_sql(file, file_ext):
    """
    Process and store a dataset (CSV, Excel, or PDF) into the SQL database.
    """
    try:
        engine = get_sql_engine()
        logger.info(f"Database engine initialized for SQL")

        # Load file into a pandas DataFrame based on file type
        if file_ext == 'csv':
            logger.info(f"Reading CSV file: {file.filename}")
            df = pd.read_csv(file.file)
        elif file_ext == 'xlsx':
            logger.info(f"Reading Excel file: {file.filename}")
            df = pd.read_excel(file.file)
        elif file_ext == 'pdf':
            logger.info(f"Processing PDF file: {file.filename}")
            try:
                with pdfplumber.open(file.file) as pdf:
                    text = ''.join([page.extract_text() for page in pdf.pages])
                df = parse_pdf_to_csv(text)
            except Exception as e:
                logger.error(f"Error parsing PDF: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to parse PDF: {str(e)}")
        else:
            raise ValueError("Unsupported file type for SQL storage")

        # Save DataFrame to the database
        table_name = file.filename.split('.')[0].replace(" ", "_")
        logger.info(f"Saving data to SQL table: {table_name}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Data saved to table {table_name}, row count: {len(df)}")

        return {"table_name": table_name, "row_count": len(df)}
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error saving to SQL: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving to SQL: {str(e)}")


async def store_in_mongodb(file):
    try:
        # Initialize MongoDB client
        client = get_mongo_client()
        db = client['datasets']
        collection_name = file.filename.split('.')[0]

        # Read and parse JSON file
        logging.info(f"Reading JSON file: {file.filename}")
        data = json.load(file.file)

        # Process the JSON structure
        if isinstance(data, dict):
            # Convert the dictionary into a list of documents for MongoDB
            documents = [{"key": key, "value": value} for key, value in data.items()]
            # Special handling for nested dictionaries (e.g., "rates")
            if "rates" in data:
                rates = [{"currency": k, "rate": v} for k, v in data["rates"].items()]
                documents.append({"rates": rates})
        elif isinstance(data, list):
            # Data is already a list of documents
            documents = data
        else:
            raise ValueError("Unsupported JSON format")

        # Insert documents into MongoDB
        logging.info(f"Inserting data into MongoDB collection: {collection_name}")
        if documents:
            db[collection_name].insert_many(documents)
        else:
            raise ValueError("JSON file did not contain any documents to insert")

        return {"collection_name": collection_name, "document_count": len(documents)}

    except Exception as e:
        logging.error(f"Error in store_in_mongodb: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving to MongoDB: {e}")


def parse_pdf_to_csv(pdf_text):
    """
    Parse the text extracted from a PDF into a DataFrame.
    Handles tabular data found in the PDF text.
    """
    try:
        lines = pdf_text.split('\n')
        headers = None
        rows = []
        for line in lines:
            if not line.strip():
                continue
            if headers is None and ('County' in line and 'Registered Vehicles' in line):
                headers = line.split()
                continue
            if headers:
                row = line.split()
                if len(row) == len(headers):
                    rows.append(row)
                else:
                    logger.warning(f"Skipping malformed row: {row}")
        if headers is None or not rows:
            raise ValueError("No valid headers or rows found in the PDF.")
        return pd.DataFrame(rows, columns=headers)
    except Exception as e:
        logger.error(f"Error parsing PDF to CSV: {e}")
        raise ValueError("Failed to parse the PDF into a structured DataFrame.")
