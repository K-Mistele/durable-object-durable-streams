-- engine-design.md "Tables". In a Durable Object, `stream` always holds the object's one path.
CREATE TABLE streams (
    stream            TEXT    PRIMARY KEY,
    generation        INTEGER NOT NULL,
    content_type      TEXT    NOT NULL,
    tail              INTEGER NOT NULL,
    closed            INTEGER NOT NULL,
    closed_by         TEXT,
    ttl_seconds       INTEGER,
    expires_at        TEXT,
    created_at        TEXT    NOT NULL,
    last_accessed_at  TEXT    NOT NULL,
    last_stream_seq   TEXT
);

CREATE TABLE chunks (
    stream      TEXT    NOT NULL,
    msg_offset  INTEGER NOT NULL,
    part        INTEGER NOT NULL,
    data        BLOB    NOT NULL,
    PRIMARY KEY (stream, msg_offset, part)
);

CREATE TABLE producers (
    stream    TEXT    NOT NULL,
    id        TEXT    NOT NULL,
    epoch     INTEGER NOT NULL,
    last_seq  INTEGER NOT NULL,
    PRIMARY KEY (stream, id)
);
