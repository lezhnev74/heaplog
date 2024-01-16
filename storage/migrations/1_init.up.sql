CREATE TABLE files
(
    path     STRING UNIQUE NOT NULL,
    fileHash STRING        NOT NULL,
    size     BIGINT        NOT NULL
);

CREATE SEQUENCE segment_ids;
CREATE TABLE file_segments
(
    id       UINTEGER NOT NULL, -- unique id of every segment
    fileHash STRING NOT NULL, -- hash of the file path
    posFrom  BIGINT NOT NULL,
    posTo    BIGINT NOT NULL,
    dateMin  BIGINT NOT NULL,
    dateMax  BIGINT NOT NULL
);

CREATE TABLE file_segments_messages
(
    id         BIGINT NOT NULL,
    segment_id UINTEGER NOT NULL,
    posFrom    BIGINT NOT NULL,
    posTo      BIGINT NOT NULL,
    date       BIGINT NOT NULL,
);

CREATE TABLE file_segments_messages_tail
(
    message_id BIGINT NOT NULL
);

CREATE TABLE file_segments_terms -- II
(
    term_id    UINTEGER NOT NULL, -- unique id of every term (assigned in the Terms)
    segment_id UINTEGER   NOT NULL, -- file_segments.id
);

CREATE TABLE queries
(
    query     STRING NOT NULL,
    queryHash STRING NOT NULL, -- hash of the query for unique checks
    dateMin   BIGINT,
    dateMax   BIGINT,
    builtDate BIGINT,
    lastRead  BIGINT           -- used in eviction
);

CREATE TABLE query_results
(
    queryHash  STRING NOT NULL,
    message_id BIGINT NOT NULL -- see file_segments_messages.id
);