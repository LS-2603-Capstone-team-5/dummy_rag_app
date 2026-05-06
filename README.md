To set-up:
1) create .env in line with .env.example in both frontend and backend folders
2) npm install in frontend
3) npm run dev in frontend to run frontend server
4) poetry config virtualenvs.in-project true THEN poetry install in backend then env $(poetry env activate)
5) Create lecture_rag db in postgres
6) Enable vectors in psql with, CREATE EXTENSION IF NOT EXISTS vector;
7) create conversations table in postgres  from sql in backend (src/models/conversations.sql)
 -- you can skip the cache table since that feature is disabled right now
 -- don't need to make a data_chunks table; cocoIndex will make one automatically
8) poetry run python src/data/cocoindex_pipeline.py to populate the coco_data_chunks table with chunks
9) poetry run python src/data/cocoindex_pipeline.py --live to run the live updater
10) poetry run python app.py to run backend server