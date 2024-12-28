from sqlalchemy import create_engine, Date, text
from sqlalchemy.types import String
from langchain_openai import ChatOpenAI
import pandas as pd
import re
from app.metadata_llm import MetadataManager

class LLMService:
    def __init__(self, api_key, db_url, metadata_path):
        datasets_db_url = db_url.replace("app_db", "datasets")
        self.llm = ChatOpenAI(model="gpt-4", temperature=0, openai_api_key=api_key)
        self.engine = create_engine(db_url)
        self.metadata_manager = MetadataManager(metadata_path)
        self.metadata_manager.load_metadata()
        self.dataset_metadata = self.metadata_manager.get_metadata()

    def load_dataset_in_chunks(self, file_path, chunk_size=10000):
        """Load a dataset from a CSV file in chunks."""
        try:
            return pd.read_csv(file_path, chunksize=chunk_size)
        except Exception as e:
            print(f"Error loading dataset: {e}")
            raise ValueError(f"Error loading dataset in chunks: {e}")

    def load_dataset(self, file_path):
        """Load dataset from CSV."""
        try:
            data = pd.read_csv(file_path)
            return data
        except Exception as e:
            raise ValueError(f"Error loading dataset: {e}")

    def store_data_in_sql(self, file_path, table_name, chunk_size=20000):
        """Store the dataset in the PostgreSQL database."""
        try:
            total_rows = 0
            for idx, chunk in enumerate(self.load_dataset_in_chunks(file_path, chunk_size)):
                if "Date" in chunk.columns:
                    chunk["Date"] = pd.to_datetime(chunk["Date"], format="%m/%d/%Y", errors="coerce").dt.date
                chunk.columns = [col.lower() for col in chunk.columns]
                # Store the dataset in PostgreSQL
                chunk.to_sql(
                    table_name,
                    con=self.engine,
                    if_exists="append",
                    index=False,
                    method= "multi"
                )
                total_rows += len(chunk)
                print(f"Chunk {idx + 1}: {len(chunk)} rows written to {table_name}. Total so far: {total_rows} rows.")
            print(f"Upload complete. Total rows written to {table_name}: {total_rows}.")
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

        print(f"DEBUG: Raw query before cleaning:\n{query}")
        
        # Remove SQL code block delimiters (```sql ... ```)
        if "```sql" in query:
            query = query.split("```sql")[-1].split("```")[0].strip()

        # Quote all column names to preserve case sensitivity
        column_names = [col.lower() for col in self.dataset_metadata["columns"].keys()]
        for column in column_names:
            query = re.sub(
                rf"(?<!\")\b{column}\b(?!\")",
                f'"{column}"',
                query,
                flags=re.IGNORECASE
            )

        # Replace MySQL DATE_FORMAT with PostgreSQL TO_CHAR
        query = re.sub(
            r"DATE_FORMAT\((.+?),\s*'%Y/%m'\)",
            r"TO_CHAR(\1, 'YYYY/MM')",
            query,
            flags=re.IGNORECASE
        )

        # Define time_filter_column
        time_filter_column = self.dataset_metadata.get("time_filter_column", "Date").lower()

        # Replace LIKE patterns for dates with range comparisons
        query = re.sub(
            rf"\"{time_filter_column}\" LIKE '(\d{{4}})%%'",
            rf"\"{time_filter_column}\" >= '\1-01-01' AND \"{time_filter_column}\" < '\1-01-01'::date + interval '1 year'",
            query,
            flags=re.IGNORECASE,
        )
        query = re.sub(
            rf"\"{time_filter_column}\" LIKE '(\d{{4}})/(\d{{2}})%%'",
            rf"\"{time_filter_column}\" >= '\1-\2-01' AND \"{time_filter_column}\" < '\1-\2-01'::date + interval '1 month'",
            query,
            flags=re.IGNORECASE,
        )
        # Catch-all for unhandled LIKE with date columns
        query = re.sub(
            rf"\"{time_filter_column}\" LIKE '(.+?)%'",
            lambda match: f"TO_CHAR(\"{time_filter_column}\", 'YYYY/MM/DD') LIKE '{match.group(1)}%'",
            query,
            flags=re.IGNORECASE,
        )

        # Replace YEAR() with EXTRACT(YEAR FROM ...)
        query = re.sub(
            rf"YEAR\(\"{time_filter_column}\"\)",
            f"EXTRACT(YEAR FROM \"{time_filter_column}\")",
            query,
            flags=re.IGNORECASE,
        )

        # Replace redundant LOWER() calls
        query = re.sub(r"LOWER\(LOWER\((.+?)\)\)", r"LOWER(\1)", query)

        # Simplify numeric comparisons wrapped with LOWER()
        query = re.sub(r"LOWER\((EXTRACT\(.+?\))\) = LOWER\('(\d+)'\)", r"\1 = \2", query)

        # Ensure consistent capitalization for string literals
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

        # Handle MAX with additional columns via CTE
        if "MAX(" in query and "," in query:
            query = re.sub(
                rf"SELECT MAX\((.+?)\), (.+?), (.+?)\s+FROM (.+?)\s+WHERE (.+)",
                rf"WITH ranked_commissions AS (SELECT \2, \3, \1, ROW_NUMBER() OVER (ORDER BY \1 DESC) AS r FROM \4 WHERE \5) "
                rf"SELECT \2, \3, \1 FROM ranked_commissions WHERE r = 1",
                query,
                flags=re.IGNORECASE,
            )

        # Remove any trailing semicolon in the WITH clause
        query = re.sub(
            r"WITH ranked_commissions AS \((.*?);?\)",
            r"WITH ranked_commissions AS (\1)",
            query,
            flags=re.IGNORECASE | re.DOTALL,
            )
        
        # Remove GROUP BY when ROW_NUMBER() is used
        if "ROW_NUMBER()" in query:
            query = re.sub(r"GROUP BY .+?\n", "", query, flags=re.IGNORECASE)
            query = re.sub(r"ORDER BY MAX\(.+?\)\s*DESC", "", query, flags=re.IGNORECASE)

        print(f"DEBUG: Cleaned query: \n{query}")
        
        return query

    def execute_query(self, query):
        """Execute the SQL query and fetch results."""
        try:
            if not isinstance(query, str):
                query = str(query)
            print(f"Executing SQL query: {query}")
            with self.engine.connect() as connection:
                with connection.begin():
                    result = connection.execute(text(query))
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