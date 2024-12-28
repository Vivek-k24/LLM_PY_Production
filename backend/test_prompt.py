import os
from app.llm_service import LLMService
from app.metadata_llm import MetadataManager

# Initialize LLMService
API_KEY = os.getenv("OPENAI_API_KEY")
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123456@localhost:5432/datasets")
METADATA_PATH = os.path.join(os.getcwd(), "Datasets", "dataset_metadata.json")
llm_service = LLMService(api_key=API_KEY, db_url=DB_URL, metadata_path=METADATA_PATH)

# Prompts to test
prompts = [
    "How many cars were sold in May 2023?",
    "What is the total sale price of cars sold by Lee Wilson?",
    "How many Chevrolet cars were sold in 2022?",
    "List all car models sold by Jason Jackson in 2023.",
    "What is the average commission earned for cars sold in 2023?",
    "How many unique customers bought Ford cars?",
    "Which salesperson sold the most Honda cars?",
    "What is the total revenue generated from Civic car sales?",
    "List all car sales made in December 2022.",
    "What is the highest sale price of a car in 2023?",
    "How many cars were sold in 2022?",
    "Which car model had the highest average sale price in 2023?",
    "What is the total commission earned by Stephanie Barber?",
    "What is the most popular car make sold by Christopher Cline?",
    "List all sales made to customers named 'David'.",
    "What is the total sale price for cars sold by Brenda Wilson in 2015?",
    "How many cars were sold by each salesperson in 2023?",
    "What is the average sale price of Chevrolet cars sold in 2022?",
    "How many Civic cars were sold by Jessica Wong?",
    "What is the total revenue generated from Nissan cars?",
    "What is the average commission rate for cars sold in 2023?",
    "List all sales with a sale price greater than 30,000 in 2022.",
    "Which salesperson generated the highest total revenue in 2023?",
    "What is the total commission earned for Ford cars?",
    "How many cars sold in 2022 were manufactured in 2010?",
    "What is the lowest sale price for a car sold in 2023?",
    "Which car model had the highest total sales revenue in 2023?",
    "What is the total revenue generated from Silverado car sales?",
    "How many Altima cars were sold in 2022?",
    "Which salesperson sold the least number of cars in 2023?",
    "What is the total revenue generated from cars sold by April Clarke?",
    "How many cars were sold in 2023 by Heather Sutton?",
    "What is the average sale price for Altima cars sold in 2022?",
    "Which car make had the highest total revenue in 2023?",
    "List all sales made by Angela Mcclain in 2022.",
    "What is the average commission earned for cars sold by Scott Gutierrez?",
    "How many cars were sold by each salesperson in 2022?",
    "What is the total revenue generated from cars sold in 2023?",
    "How many cars manufactured in 2016 were sold in 2022?",
    "What is the average sale price for Silverado cars sold by Wesley Snow?",
    "Which salesperson had the highest average sale price in 2023?",
    "List all sales where the commission earned was greater than 5,000 in 2022.",
    "How many unique customers bought cars in 2023?",
    "What is the total sale price for cars sold in December 2023?",
    "List all sales made by Tina Zimmerman in 2022.",
    "What is the highest commission earned for a car sold in 2023?",
    "How many cars manufactured in 2020 were sold in 2023?",
    "Which salesperson had the highest total commission in 2022?",
    "What is the most popular car model sold in 2023?",
]

# File to save responses
response_file_path = os.path.join(os.getcwd(), "llm_response.txt")

# Test LLM with prompts
successful_prompts = []
failed_prompts = []

for prompt in prompts:
    try:
        refined_prompt = llm_service.generate_dynamic_prompt(prompt)
        generated_sql = llm_service.generate_sql_query(refined_prompt)
        results = llm_service.execute_query(generated_sql)
        successful_prompts.append({"prompt": prompt, "generated_sql": generated_sql, "results": results})
    except Exception as e:
        failed_prompts.append({"prompt": prompt, "error": str(e)})
        print(f"DEBUG: Failed Prompt: {prompt}\nError: {e}\n")

# Write results to file
with open(response_file_path, "w") as f:
    f.write("Successful Prompts:\n")
    for success in successful_prompts:
        f.write(f"Prompt: {success['prompt']}\n")
        f.write("Status: Success\n\n")

    f.write("\nFailed Prompts:\n")
    for failure in failed_prompts:
        f.write(f"Prompt: {failure['prompt']}\n")
        f.write(f"Error: {failure['error']}\n\n")

print(f"Testing complete. Results saved to {response_file_path}")