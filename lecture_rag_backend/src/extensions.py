import os
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
DB_CONN_STRING = os.getenv("PSYCOPG_CONNECTION_STRING")
