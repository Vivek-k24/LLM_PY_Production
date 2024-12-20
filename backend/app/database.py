from sqlalchemy import create_engine
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

SQL_DB = os.getenv('DATABASE_URL')
MONGO_URI = os.getenv('MONGO_URI')

def get_sql_engine():
    return create_engine("mssql+pyodbc://sa:YourStrong!Passw0rd@db/datasets?driver=ODBC+Driver+17+for+SQL+Server")

def get_mongo_client():
    return MongoClient("mongodb://localhost:27017/")
