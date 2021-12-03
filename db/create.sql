CREATE TABLE IF NOT EXISTS DealLogs (
    DealUUID TEXT,
    CreatedAt DateTime,
    LogText TEXT
);

CREATE TABLE IF NOT EXISTS Deals (
    ID TEXT,
    CreatedAt DateTime,
    DealProposalSignature BLOB,
    PieceCID TEXT,
    PieceSize INT,
    VerifiedDeal BOOL,
    ClientAddress TEXT,
    ProviderAddress TEXT,
    Label TEXT,
    StartEpoch INT,
    EndEpoch INT,
    StoragePricePerEpoch TEXT,
    ProviderCollateral TEXT,
    ClientCollateral TEXT,
    SelfPeerID TEXT,
    ClientPeerID TEXT,
    DealDataRoot TEXT,
    InboundFilePath TEXT,
    TransferType TEXT,
    TransferParams BLOB,
    TransferSize INT,
    ChainDealID INT,
    PublishCID TEXT,
    SectorID INT,
    Offset INT,
    Length INT,
    Checkpoint TEXT,
    Error TEXT,
    PRIMARY KEY(ID)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS FundsLogs (
    DealUUID TEXT,
    CreatedAt DateTime,
    Amount TEXT,
    LogText TEXT
);

CREATE TABLE IF NOT EXISTS FundsTagged (
    DealUUID TEXT,
    CreatedAt DateTime,
    Collateral TEXT,
    PubMsg TEXT
);

CREATE TABLE IF NOT EXISTS StorageLogs (
    DealUUID TEXT,
    CreatedAt DateTime,
    PieceSize TEXT,
    LogText TEXT
);

CREATE TABLE IF NOT EXISTS StorageTagged (
    DealUUID TEXT,
    CreatedAt DateTime,
    PieceSize TEXT
);
