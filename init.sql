DROP TABLE IF EXISTS matches;

CREATE TABLE matches (
  id TEXT PRIMARY KEY,
  creator_name TEXT,
  sport TEXT,
  date DATE,
  time TEXT,
  note TEXT,
  join_requests JSON,
  hotel TEXT
);
