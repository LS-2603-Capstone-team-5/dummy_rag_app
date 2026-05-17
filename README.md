To set-up:
1) create .env in line with .env.example in both frontend and backend folders
2) npm install in frontend
3) npm run dev in frontend to run frontend server
4) poetry config virtualenvs.in-project true THEN poetry install in backend then eval $(poetry env activate)
5) Create lecture_rag db in postgres
6) Enable vectors in psql with, CREATE EXTENSION IF NOT EXISTS vector;
7) create conversations table in postgres from sql in backend (src/models/conversations.sql)
8) create data_chunks table in postgres from sql in backend (src/models/data_chunks.sql)
 -- you can skip the cache table since that feature is disabled right now
9) wire up the functions in INGESTION_PIPELINE_SPEC to SpruceUp
9) poetry run python app.py to run backend server