from sqlalchemy import create_engine, Date
from sqlalchemy.types import String
from langchain_openai import ChatOpenAI
import pandas as pd
import re
from app.metadata_llm import MetadataManager

class LLMService:
    def __init__(self, api_key, db_url, metadata_path):
        self.llm = ChatOpenAI(model="gpt-4", temperature=0, openai_api_key=api_key)
        self.engine = create_engine(db_url)
        self.metadata_manager = MetadataManager(metadata_path)
        self.metadata_manager.load_metadata()
        self.dataset_metadata = self.metadata_manager.get_metadata()

    def load_dataset(self, file_path):
        """Load dataset from CSV."""
        try:
            data = pd.read_csv(file_path)
            return data
        except Exception as e:
            raise ValueError(f"Error loading dataset: {e}")

    def store_data_in_sql(self, data, table_name):
        """Store the dataset in the PostgreSQL database."""
        try:
            if "Date" in data.columns:
            # Convert 'Date' to PostgreSQL-compatible format
                data["Date"] = pd.to_datetime(data["Date"], format="%m/%d/%Y", errors="coerce").dt.date
            
            data.columns = [col.lower() for col in data.columns]

        # Store the dataset in PostgreSQL
            data.to_sql(
            table_name,
            con=self.engine,
            if_exists="replace",
            index=False,
            method= "multi"
        )
        except Exception as e:
            raise ValueError(f"Error storing data in PostgreSQL: {e}")



    def generate_dynamic_prompt(self, user_query):
        """Generate a refined prompt dynamically based on the dataset metadata."""
        metadata = self.dataset_metadata
        dataset_name = metadata["dataset_name"]
        column_info = "\n".join([f"- {col}: {desc}" for col, desc in metadata["columns"].items()])
    
        refined_prompt = (
        f"You are a helpful assistant tasked with analyzing and modifying data.\n"
        f"Dataset: '{dataset_name}'\n"
        f"Columns:\n{column_info}\n"
        f"Purpose: {metadata['purpose']}\n\n"
        f"Important: You are restricted to using only the '{dataset_name}' table "
        f"and its available columns for all operations.\n"
        f"Important: When filtering by year, month, or day, use the '{metadata.get('time_filter_column', 'Date')}' column.\n"
        f"Ensure string comparisons are case-insensitive and match exactly.\n"
        f"User query: {user_query}\n\n"
        f"Generate only the SQL query required to execute this operation. Do not include explanatory text."
        )
        return refined_prompt

    def generate_sql_query(self, prompt):
        """Generate SQL query using LLM based on the prompt."""
        try:
            response = self.llm.invoke(prompt)
            raw_query = response.content.strip()
            return self._clean_sql_query(raw_query)
        except Exception as e:
            raise ValueError(f"Error generating SQL query: {e}")

    def _clean_sql_query(self, query):
        """Clean and format SQL query for PostgreSQL."""
        # Remove SQL code block delimiters (```sql ... ```)
        if "```sql" in query:
            query = query.split("```sql")[-1].split("```")[0].strip()
        
        # Quote all column names to preserve case sensitivity
        column_names = self.dataset_metadata["columns"].keys()
        for column in column_names:
            if column not in query:
                continue
            query = re.sub(
                rf"(?<!\")\b{column}\b(?!\")",
                f'"{column}"',
                query
            )

        # Replace SQLite-specific functions with PostgreSQL equivalents
        time_filter_column = self.dataset_metadata.get("time_filter_column", "Date")
        if time_filter_column in query:
            query = re.sub(
                rf"strftime\('%m', {time_filter_column}\) = '(\d+)' AND strftime\('%Y', {time_filter_column}\) = '(\d+)'",
                rf"EXTRACT(MONTH FROM \"{time_filter_column}\") = \1 AND EXTRACT(YEAR FROM \"{time_filter_column}\") = \2",
                query,
            )

        # Remove redundant LOWER() calls for numeric fields
        query = re.sub(r"LOWER\((EXTRACT\(.+?)\) = LOWER\('(\d+)'\)", r"\1 = \2", query)

        # Ensure consistent capitalization for literals
        query = re.sub(
            r"= '([^']*)'",
            lambda match: f"= '{match.group(1).capitalize()}'",
            query,
        )

        # Ensure case-insensitive string comparisons for PostgreSQL
        query = re.sub(
            r"WHERE (.+?) = '(.+?)'",
            lambda match: f"WHERE LOWER({match.group(1)}) = LOWER('{match.group(2)}')",
            query,
        )
        return query



    def execute_query(self, query):
        """Execute the SQL query and fetch results."""
        try:
            print(f"Executing SQL query: {query}")
            with self.engine.connect() as connection:
                with connection.begin():
                    result = connection.execute(query)
                    if result.returns_rows:
                        rows = result.fetchall()
                        return [dict(zip(result.keys(), row)) for row in rows]
                return {"message": f"Query executed successfully. Rows affected: {result.rowcount}"}
        except Exception as e:
            print(f"Query Execution Error: {e}")
            raise ValueError(f"Error executing query: {e}")

    def main_process(self, dataset_path, table_name):
        """Main process to load dataset, store it, and connect to SQL."""
        try:
            data = self.load_dataset(dataset_path)
            self.store_data_in_sql(data, table_name)
        except Exception as e:
            raise ValueError(f"An error occurred: {e}")