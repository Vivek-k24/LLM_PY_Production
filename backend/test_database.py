from sqlalchemy import create_engine
from sqlalchemy.sql import text

engine = create_engine("mssql+pyodbc://sa:YourStrong!Passw0rd@llm_py_production-db-1:1433/datasets?driver=ODBC+Driver+17+for+SQL+Server")


with engine.connect() as conn:
    print("Connection successful!")