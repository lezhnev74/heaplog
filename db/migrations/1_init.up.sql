CREATE SEQUENCE file_ids;
CREATE TABLE files
(
    id   UINTEGER      NOT NULL,
    path STRING UNIQUE NOT NULL
);
CREATE SEQUENCE segment_ids;
CREATE TABLE file_segments
(
    id      UINTEGER NOT NULL, -- unique id of every segment
    fileId  UINTEGER NOT NULL,
    posFrom UBIGINT  NOT NULL, -- [from, to)
    posTo   UBIGINT  NOT NULL,
    dateMin UBIGINT  NOT NULL, -- first message's date (micro)
    dateMax UBIGINT  NOT NULL  -- last message's date (micro)
);
CREATE TABLE file_segments_messages
(
    segmentId   UINTEGER NOT NULL,
    posFrom     UBIGINT  NOT NULL,
    relDateFrom UTINYINT NOT NULL, -- [from, to), relative to posFrom
    relDateTo   UTINYINT NOT NULL  -- dates loc is kept to exclude from matching
);
CREATE SEQUENCE query_ids;
CREATE TABLE queries
(
    queryId  UINTEGER NOT NULL,
    text     STRING   NOT NULL,
    dateMin  UBIGINT  NOT NULL,
    dateMax  UBIGINT  NOT NULL,
    messages UINTEGER NOT NULL,
    finished BOOL,
    builtAt  UBIGINT  NOT NULL
);
CREATE TABLE query_results -- contains all info to avoid joins for faster pagination
(
    queryId UINTEGER NOT NULL,
    fileId  UINTEGER NOT NULL,
    pos     UBIGINT  NOT NULL,
    len     UINTEGER NOT NULL,
    date    UBIGINT  NOT NULL -- micro
);