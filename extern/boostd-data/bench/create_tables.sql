CREATE TABLE PayloadToPieces (
    PayloadMultihash bytea PRIMARY KEY,
    PieceCids bytea
);

CREATE TABLE PieceBlockOffsetSize (
    PieceCid bytea,
    PayloadMultihash bytea,
    BlockOffset BIGINT,
    BlockSize BIGINT,
    PRIMARY KEY (PieceCid, PayloadMultihash)
);
