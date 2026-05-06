CREATE TABLE conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    history JSONB NOT NULL DEFAULT '[]',
    summary text,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    llm_context JSONB NOT NULL DEFAULT '[]'
);


--Basic insert
INSERT INTO conversations (history)
VALUES ('[{"role": "user", "content": "Tell me about Aristotle."}]');


--Basic update
UPDATE conversations
SET history = history || '[{"role": "assistant", "content": "Aristotle was a philosopher"}]'
WHERE id = '9f8cd020-14c0-4bda-b62f-84c17d168bd3'; -- or whatever
