-- engine-design.md "Forks".
-- fork: the PUT that made this stream a fork (source, offset and sub-offset as requested), JSON.
-- copy: set while the fork is still being created (claimed, or copying and how far), JSON.
-- soft_deleted: deleted while forks still copy from it; answers 410.
ALTER TABLE streams ADD COLUMN fork TEXT;
ALTER TABLE streams ADD COLUMN copy TEXT;
ALTER TABLE streams ADD COLUMN soft_deleted INTEGER NOT NULL DEFAULT 0;

-- on a source: one row per fork made from it
CREATE TABLE forks (
    stream  TEXT NOT NULL,
    fork    TEXT NOT NULL,
    PRIMARY KEY (stream, fork)
);

-- on a purged fork: a removeFork call to its source that failed, kept to retry
CREATE TABLE pending_releases (
    stream    TEXT    NOT NULL,
    source    TEXT    NOT NULL,
    attempts  INTEGER NOT NULL,
    due_at    INTEGER NOT NULL,   -- epoch milliseconds
    PRIMARY KEY (stream, source)
);
