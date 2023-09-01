CREATE TABLE IF NOT EXISTS PayloadToPieces (
    PayloadMultihash BLOB,
    PieceCid BLOB,
    -- PayloadMultihash is the partition key: the key that determines which
    -- node the row is sent to.
    -- PieceCid is the clustering key: the data is sorted by this key.
    -- We want both of these to be in the primary key because
    -- the piece cids should be a unique set.
    PRIMARY KEY (PayloadMultihash, PieceCid)
);

CREATE TABLE IF NOT EXISTS PieceBlockOffsetSize (
    PieceCid BLOB,
    PayloadMultihash BLOB,
    BlockOffset BIGINT,
    BlockSize BIGINT,
    PRIMARY KEY (PieceCid, PayloadMultihash)
);

CREATE TABLE IF NOT EXISTS PieceMetadata (
    PieceCid TEXT PRIMARY KEY,
    Version TEXT,
    CreatedAt TIMESTAMP,
    IndexedAt TIMESTAMP,
    CompleteIndex BOOLEAN,
    Error TEXT,
    ErrorType Text
) WITH transactions = { 'enabled' : true };

CREATE INDEX IF NOT EXISTS PieceMetadataCreatedAt ON PieceMetadata (CreatedAt);

CREATE TABLE IF NOT EXISTS PieceDeal (
    DealUuid TEXT PRIMARY KEY,
    PieceCid BLOB,
    IsLegacy BOOLEAN,
    ChainDealID BIGINT,
    MinerAddr TEXT,
    SectorID BIGINT,
    PieceOffset BIGINT,
    PieceLength BIGINT,
    CarLength BIGINT
) WITH transactions = { 'enabled' : true };

CREATE INDEX IF NOT EXISTS PieceDealPieceCid ON PieceDeal (PieceCid);
