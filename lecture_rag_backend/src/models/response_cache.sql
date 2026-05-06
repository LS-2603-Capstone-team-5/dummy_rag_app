CREATE TABLE response_cache (
  id serial PRIMARY KEY,
  query_embedding_vector vector(1536) NOT NULL,
  query_text text NOT NULL,
  response_text text NOT NULL,
  dialogue_mode boolean NOT NULL DEFAULT false,
  created_at timestamptz DEFAULT NOW()
);