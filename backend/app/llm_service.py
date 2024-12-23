from sqlalchemy import create_engine
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
        """Store dataset into SQL Server."""
        try:
            data.to_sql(table_name, con=self.engine, if_exists="replace", index=False)
        except Exception as e:
            raise ValueError(f"Error storing data in SQL Server: {e}")

    def generate_dynamic_prompt(self, user_query):
        """Generate a refined prompt dynamically based on the dataset metadata."""
        metadata = self.dataset_metadata
        dataset_name = metadata["dataset_name"]
        column_info = "\n".join([f"- {col}: {desc}" for col, desc in metadata["columns"].items()])
        
        refined_prompt = (
            f"You are a helpful assistant tasked with analyzing data.\n"
            f"Dataset: '{dataset_name}'\n"
            f"Columns:\n{column_info}\n"
            f"Purpose: {metadata['purpose']}\n\n"
            f"Important: You are restricted to using only the '{dataset_name}' table "
            f"and its available columns.\n"
            f"User query: {user_query}"
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
        """Clean and format SQL query."""
        # Remove SQL code block delimiters (```sql ... ```)
        if "```sql" in query:
            query = query.split("```sql")[-1].split("```")[0].strip()
        # Replace backticks with square brackets for SQL Server compatibility
        query = query.replace("`", "[").replace("`", "]")
        # Apply square brackets to column names with spaces or special characters
        column_names = self.dataset_metadata["columns"].keys()
        for column in column_names:
            if " " in column or not column.isidentifier():
                query = re.sub(rf"(?<!\[){column}(?!\])", f"[{column}]", query)
        # Remove square brackets around SQL keywords and aliases
        keywords = ["SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "AS", "OFFSET", "FETCH NEXT", "ROWS ONLY"]
        for keyword in keywords:
            query = re.sub(rf"\[{keyword}\]", keyword, query, flags=re.IGNORECASE)
        # Correct ORDER BY clause
        query = re.sub(r"ORDER BY \[(.+?) ASC\]", r"ORDER BY \1 ASC", query, flags=re.IGNORECASE)
        # Replace LIMIT with OFFSET...FETCH for SQL Server
        if "LIMIT" in query:
            match = re.search(r"LIMIT\s+(\d+)", query)
            if match:
                limit = match.group(1)
                query = re.sub(r"LIMIT\s+\d+", "", query)
                query = re.sub(r"ORDER BY (.+)", r"ORDER BY \1 OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY", query)

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
            raise ValueError(f"Error executing query: {e}")

    def main_process(self, dataset_path, table_name):
        """Main process to load dataset, store it, and connect to SQL."""
        try:
            data = self.load_dataset(dataset_path)
            self.store_data_in_sql(data, table_name)
        except Exception as e:
            raise ValueError(f"An error occurred: {e}")