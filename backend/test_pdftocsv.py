from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse
import os
import pandas as pd
import subprocess
from tabula import read_pdf

app = FastAPI()

# Directory to save converted files
OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Ensure tabula-java is installed
TABULA_JAR_PATH = "H:/tabula-1.0.5-jar-with-dependencies.jar"


def extract_with_tabula(file_path, output_csv):
    """
    Extract tables from a PDF using tabula-py.
    """
    try:
        subprocess.run([
            "java", "-jar", TABULA_JAR_PATH,
            "-i", "-o", output_csv, "--pages", "all", file_path
        ], check=True)
        return output_csv
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Error using Tabula: {e}")


@app.post("/parse-pdf/")
async def parse_pdf(file: UploadFile, output_format: str = "csv"):
    """
    Parse a PDF file and convert it into CSV or Excel using tabula-py or PyPDF2.

    Args:
        file (UploadFile): PDF file uploaded by the user.
        output_format (str): Output format ('csv' or 'excel').

    Returns:
        FileResponse: Response containing the converted file for download.
    """
    try:
        if not file.filename.endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Only PDF files are supported.")

        # Save the uploaded file to a temporary location
        temp_pdf_path = os.path.join(OUTPUT_DIR, file.filename)
        with open(temp_pdf_path, "wb") as f:
            f.write(await file.read())

        # Attempt to parse with Tabula
        try:
            output_file_path = os.path.join(OUTPUT_DIR, "output.csv")
            extract_with_tabula(temp_pdf_path, output_file_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed with Tabula: {str(e)}")

        # Load the CSV into pandas for Excel conversion, if needed
        if output_format.lower() == "excel":
            df = pd.read_csv(output_file_path)
            output_excel_path = os.path.splitext(output_file_path)[0] + ".xlsx"
            df.to_excel(output_excel_path, index=False)
            return FileResponse(output_excel_path,
                                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                filename="output.xlsx")

        # Default to returning the CSV file
        return FileResponse(output_file_path, media_type="text/csv", filename="output.csv")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse PDF: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
