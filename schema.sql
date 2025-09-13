-- SQLite スキーマ（同梱）
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  name TEXT,
  parent_path TEXT,
  file_type TEXT,
  size INTEGER,
  dev INTEGER,
  inode INTEGER,
  mtime INTEGER,
  nlink INTEGER,
  scanned_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent_path);
CREATE INDEX IF NOT EXISTS idx_files_dev_inode ON files(dev, inode);

CREATE TABLE IF NOT EXISTS dir_stats (
  dir_path TEXT PRIMARY KEY,
  total_size INTEGER,
  file_count INTEGER,
  last_scanned INTEGER
);