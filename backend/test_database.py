from sqlalchemy import create_engine, text

DATABASE_URL = "mssql+pyodbc://AnonymousUser:@llm_py_production-db-1:1433/datasets?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=no"
engine = create_engine(DATABASE_URL)

query = """
SELECT * 
FROM dbo.car_sales_data
WHERE ([CarModel] = 'Civic' AND [CarMake] = 'Honda')
   OR ([CarModel] = 'F-150' AND [CarMake] = 'Ford')
   OR ([CarModel] = 'Altima' AND [CarMake] = 'Nissan')
   OR ([CarModel] = 'Corolla' AND [CarMake] = 'Toyota');
"""

try:
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        print("Rows fetched:", rows)
except Exception as e:
    print("Query failed:", e)
