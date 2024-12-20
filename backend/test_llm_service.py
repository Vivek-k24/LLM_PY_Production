import pandas as pd
from sqlalchemy import create_engine
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain_community.chat_models import ChatOpenAI
import os


# OpenAI API key
OPENAI_API_KEY = "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"

# Database configuration
DATABASE_URL = "mssql+pyodbc://sa:YourStrong!Passw0rd@llm_py_production-db-1:1433/datasets?driver=ODBC+Driver+17+for+SQL+Server"

# File path to the dataset
DATASET_PATH = "/app/Datasets/car_sales_data.csv"

def load_dataset(file_path):
    """Load the dataset from a CSV file."""
    data = pd.read_csv(file_path)
    print("Dataset Loaded Successfully")
    print("Columns in the dataset: ", data.columns)
    return data

def connect_to_sql_server():
    """Connect to the Microsoft SQL Server database."""
    engine = create_engine(DATABASE_URL)
    print("Connected to SQL Server")
    return engine

def store_data_in_sql(data, engine, table_name="car_sales"):
    """Store the dataset into a SQL Server database."""
    data.to_sql(table_name, con=engine, if_exists="replace", index=False)
    print(f"Data stored in SQL Server table: {table_name}")

def preprocess_data(data):
    """Perform preprocessing and generate summarized insights."""
    # Correct column names based on the dataset
    total_commission = data["Commission Earned"].sum() 

    # Sales by car make
    sales_by_make = data.groupby("Car Make")["Commission Earned"].sum().reset_index()

    # Top salesperson
    top_salesperson = (
        data.groupby("Salesperson")["Commission Earned"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .iloc[0]
    )

    # Summarized data
    summary = {
        "total_commission": total_commission,
        "sales_by_make": sales_by_make.to_dict(orient="records"),
        "top_salesperson": {
            "name": top_salesperson["Salesperson"],
            "commission": top_salesperson["Commission Earned"],
        },
    }
    return summary


def query_llm(summary):
    """Query the LLM for simplified answers based on the summarized data."""
    # Initialize the LLM
    llm = ChatOpenAI(api_key=OPENAI_API_KEY, model="gpt-3.5-turbo")

    # Define the prompt
    prompt = PromptTemplate(
        input_variables=["data_summary"],
        template=(
            "The sales dataset contains information on salespeople, car makes, commission percentages, "
            "and commission earned. Here is the summarized data: {data_summary}. "
            "Provide insights like total commission earned, top salesperson, and the car make with the highest sales."
        ),
    )

    # Combine the prompt and LLM in a chain
    chain = prompt | llm  # Correct usage with RunnableSequence

    # Query the LLM
    response = chain.invoke({"data_summary": summary})
    print("LLM Response:", response.content)  # Access the chat response content
    return response.content

def main():
    # Step 1: Load the dataset
    data = load_dataset(DATASET_PATH)

    # Step 2: Connect to SQL Server
    engine = connect_to_sql_server()

    # Step 3: Store the dataset in SQL Server
    store_data_in_sql(data, engine)

    # Step 4: Preprocess the dataset
    summary = preprocess_data(data)
    print("Data Summary:", summary)

    # Step 5: Query the LLM for insights
    llm_response = query_llm(summary)

    # Output the response to a file
    with open("llm_response.txt", "w") as f:
        f.write(llm_response)

if __name__ == "__main__":
    main()
