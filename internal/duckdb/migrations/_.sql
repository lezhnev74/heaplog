CREATE SEQUENCE file_ids;
CREATE TABLE files
(
    id   UINTEGER      NOT NULL,
    path STRING UNIQUE NOT NULL
);
CREATE SEQUENCE segment_ids;
CREATE TABLE segments
(
    id            UINTEGER NOT NULL, -- unique id of every segment
    file_id       UINTEGER NOT NULL,
    pos_from      UBIGINT  NOT NULL, -- [from, to)
    pos_to        UBIGINT  NOT NULL,
    date_min      UBIGINT  NOT NULL, -- first message's date (micro)
    date_max      UBIGINT  NOT NULL, -- last message's date (micro)
);
CREATE TABLE messages
(
    segment_id    UINTEGER NOT NULL,
    rel_from      UINTEGER NOT NULL, -- relative to the segment's pos
    rel_date_from UINTEGER NOT NULL, -- relative to the segment's pos
    rel_date_to   UINTEGER NOT NULL, -- relative to the segment's pos
    date          UBIGINT  NOT NULL  -- micro
);
CREATE SEQUENCE query_ids;
CREATE TABLE queries
(
    queryId  UINTEGER NOT NULL,
    text     STRING   NOT NULL,
    date_min UBIGINT  NOT NULL,
    date_max UBIGINT  NOT NULL,
    messages UINTEGER NOT NULL,
    finished BOOL,
    built_at UBIGINT  NOT NULL
);
CREATE TABLE query_results -- contains all info to avoid joins for faster pagination
(
    query_id UINTEGER NOT NULL,
    file_id  UINTEGER NOT NULL,
    pos      UBIGINT  NOT NULL,
    len      UINTEGER NOT NULL,
    date     UBIGINT  NOT NULL -- micro
);