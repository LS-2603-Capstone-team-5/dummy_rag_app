To set-up:
1) create .env in line with .env.example in both frontend and backend folders
2) npm install in frontend
3) poetry install in backend then env $(poetry env activate)
4) poetry run python app.py to run backend server
5) npm run dev in frontend to run frontend server
6) create postgres tables from sql in backend (src/models)
 -- you can skip the cache table since that feature is disabled right now
 -- the data_chunks table should be called coco_data_chunks for things to work
7) poetry run python src/data/cocoindex_pipeline.py to populate the coco_data_chunks table with chunks
8) poetry run python src/data/cocoindex_pipeline.py --live to run the live updater