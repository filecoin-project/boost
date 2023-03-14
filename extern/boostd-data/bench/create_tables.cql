create keyspace if not exists bench with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE bench.PayloadToPieces (
    PayloadMultihash BLOB PRIMARY KEY,
    PieceCids BLOB
);

CREATE TABLE bench.PieceBlockOffsetSize (
    PieceCid BLOB,
    PayloadMultihash BLOB,
    BlockOffset BIGINT,
    BlockSize BIGINT,
    PRIMARY KEY (PieceCid, PayloadMultihash)
);
