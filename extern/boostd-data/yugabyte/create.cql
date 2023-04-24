create keyspace if not exists idx with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE idx.PayloadToPieces (
    PayloadMultihash BLOB PRIMARY KEY,
    PieceCids BLOB
);

CREATE TABLE idx.PieceBlockOffsetSize (
    PieceCid BLOB,
    PayloadMultihash BLOB,
    BlockOffset BIGINT,
    BlockSize BIGINT,
    PRIMARY KEY (PieceCid, PayloadMultihash)
);

CREATE TABLE idx.PieceMetadata (
    PieceCid BLOB PRIMARY KEY,
    Version TEXT,
    IndexedAt TIMESTAMP,
    CompleteIndex BOOLEAN,
    Error TEXT,
    ErrorType Text
);

CREATE TABLE idx.PieceDeal (
    DealUuid TEXT PRIMARY KEY,
    PieceCid BLOB,
    IsLegacy BOOLEAN,
    ChainDealID BIGINT,
    MinerAddr TEXT,
    SectorID BIGINT,
    PieceOffset BIGINT,
    PieceLength BIGINT,
    CarLength BIGINT
);
