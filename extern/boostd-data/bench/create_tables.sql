CREATE TABLE IF NOT EXISTS PayloadToPieces (
    PayloadMultihash bytea PRIMARY KEY,
    PieceCids bytea
);

CREATE TABLE IF NOT EXISTS PieceBlockOffsetSize (
    PieceCid bytea,
    PayloadMultihash bytea,
    BlockOffset BIGINT,
    BlockSize BIGINT,
    PRIMARY KEY (PieceCid, PayloadMultihash)
);
