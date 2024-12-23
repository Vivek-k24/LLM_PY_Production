from sqlalchemy import create_engine, text

# Replace with your connection string
DATABASE_URL = "mssql+pyodbc://sa:Newstrongpassword123@db:1433/datasets?driver=ODBC+Driver+17+for+SQL+Server"

# Create the database engine
engine = create_engine(DATABASE_URL)

# Test the connection
with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))
    print(result.fetchall())
