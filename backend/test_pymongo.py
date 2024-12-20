from pymongo import MongoClient

# Replace with your connection string
connection_string = "mongodb://localhost:27017"

try:
    client = MongoClient(connection_string)
    print("MongoDB connection successful!")
    print("Databases:", client.list_database_names())
except Exception as e:
    print("Error connecting to MongoDB:", e)
