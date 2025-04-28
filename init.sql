CREATE TABLE IF NOT EXISTS matches (
  id TEXT PRIMARY KEY,
  creator_name TEXT NOT NULL,
  sport TEXT NOT NULL,
  date DATE NOT NULL,
  time TEXT NOT NULL,
  note TEXT,
  join_requests JSONB NOT NULL DEFAULT '[]',
  hotel TEXT NOT NULL
);