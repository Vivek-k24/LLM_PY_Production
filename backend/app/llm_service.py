from sqlalchemy import create_engine
from langchain_openai import ChatOpenAI
import re
import pandas as pd

# Predefined columns with spaces to ensure proper SQL syntax
COLUMNS_WITH_SPACES = [
    "Date",
    "CommissionEarned",
    "CommissionRate",
    "SalePrice",
    "CarMake",
    "CarModel",
    "CarYear",
    "CustomerName",
    "Salesperson"
]

class LLMService:
    def __init__(self, api_key, db_url):
        self.llm = ChatOpenAI(model="gpt-4", temperature=0, openai_api_key=api_key)
        self.engine = create_engine(db_url)

    def load_dataset(self, file_path):
        """Load dataset from CSV."""
        try:
            data = pd.read_csv(file_path)
            print("Dataset loaded successfully.")
            print("Columns:", data.columns.tolist())
            return data
        except Exception as e:
            raise ValueError(f"Error loading dataset: {e}")

    def store_data_in_sql(self, data, table_name="car_sales"):
        """Store dataset into SQL Server."""
        try:
            data.to_sql(table_name, con=self.engine, if_exists="replace", index=False)
            print(f"Data successfully stored in SQL Server table: {table_name}")
        except Exception as e:
            raise ValueError(f"Error storing data in SQL Server: {e}")

    def generate_sql_query(self, prompt):
        """Generate SQL query using LLM based on the prompt."""
        try:
            response = self.llm.invoke(prompt)
            raw_query = response.content.strip()
            return self._clean_sql_query(raw_query)
        except Exception as e:
            raise ValueError(f"Error generating SQL query: {e}")

    def _clean_sql_query(self, query):
        """Clean and format SQL query."""
        if "```sql" in query:
            query = query.split("```sql")[-1].split("```")[0].strip()

        for column in COLUMNS_WITH_SPACES:
            pattern = rf"(?<!\[){re.escape(column)}(?!\])"
            query = re.sub(pattern, f"[{column}]", query)
        
        query = query.replace("Car_Make", "[CarMake]")
        query = query.replace("Car_Model", "[CarModel]")
        query = query.replace("Car_Year", "[CarYear]")
        query = query.replace("Sale_Price", "[SalePrice]")
        query = query.replace("Customer_Name", "[CustomerName]")
        query = query.replace("Commission_Earned", "[CommissionEarned]")
        query = query.replace("Commission_Rate", "[CommissionRate]")
        query = query.replace("Car Make", "[CarMake]")
        query = query.replace("Car Model", "[CarModel]")
        query = query.replace("Car Year", "[CarYear]")
        query = query.replace("Sale Price", "[SalePrice]")
        query = query.replace("Customer Name", "[CustomerName]")
        query = query.replace("Commission Earned", "[CommissionEarned]")
        query = query.replace("Commission Rate", "[CommissionRate]")
        query = query.replace("`", "[]").replace("`","]")
        query = re.sub(r"\[\s+", "[", query)  
        query = re.sub(r"\s+\]", "]", query)
        query = re.sub(r"\[\[|\]\]", "", query)  # Remove stray brackets
        query = re.sub(r"`", "[", query).replace("]", "]")
        query = re.sub(r"\bdatasets\.", "", query)
        query = query.replace("Mapped_[CarMake]", "[Mapped_CarMake]")
        query = query.replace("Car Manufacturer", "[CarMake]")

        if "LIMIT" in query:
            limit_match = re.search(r"LIMIT\s+(\d+)", query)
            if limit_match:
                limit_value = int(limit_match.group(1))
                query = re.sub(r"LIMIT\s+\d+", "", query)  # Remove the LIMIT clause
                query = re.sub(r"SELECT", f"SELECT TOP {limit_value}", query, count=1)

        return query

    def execute_query(self, query):
        """Execute the SQL query and fetch results."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
                if result.returns_rows:
                    rows = result.fetchall()
                    results = [dict(zip(result.keys(), row)) for row in rows]
                    return results
                else:
                    raise ValueError("Query executed successfully but no rows returned")
        except Exception as e:
            raise ValueError(f"Error executing query: {e}")

    def main_process(self, dataset_path):
        """Main process to load dataset, store it, and connect to SQL."""
        try:
            print("Loading dataset...")
            data = self.load_dataset(dataset_path)

            print("Storing data in SQL Server...")
            self.store_data_in_sql(data)

            print("Dataset successfully stored in SQL Server.")
        except Exception as e:
            print(f"An error occurred: {e}")