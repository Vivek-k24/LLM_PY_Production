from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

import sqlalchemy

load_dotenv()

SQL_DB = os.getenv('DATABASE_URL')
MONGO_URI = os.getenv('MONGO_URI')

def get_sql_engine():
    return create_engine(SQL_DB)

engine = create_engine(
    os.getenv('DATABASE_URL'),
    poolclass=sqlalchemy.pool.NullPool
)