CREATE TABLE IF NOT EXISTS Deals (
    ID TEXT,
    CreatedAt DateTime,
    SignedProposalCID TEXT,
    DealProposalSignature BLOB,
    PieceCID TEXT,
    PieceSize INT,
    IsOffline BOOL,
    VerifiedDeal BOOL,
    ClientAddress TEXT,
    ProviderAddress TEXT,
    Label TEXT,
    StartEpoch INT,
    EndEpoch INT,
    StoragePricePerEpoch TEXT,
    ProviderCollateral TEXT,
    ClientCollateral TEXT,
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

CREATE INDEX IF NOT EXISTS index_deals_id on Deals(ID);
CREATE INDEX IF NOT EXISTS index_deals_publish_cid on Deals(PublishCID);
CREATE INDEX IF NOT EXISTS index_deals_signed_proposal_cid on Deals(SignedProposalCID);
CREATE INDEX IF NOT EXISTS index_deals_created_at on Deals(CreatedAt);

CREATE TABLE IF NOT EXISTS FundsLogs (
    DealUUID TEXT,
    CreatedAt DateTime,
    Amount TEXT,
    LogText TEXT
);

CREATE INDEX IF NOT EXISTS index_funds_logs_deal_uuid on FundsLogs(DealUUID);

CREATE TABLE IF NOT EXISTS FundsTagged (
    DealUUID TEXT,
    CreatedAt DateTime,
    Collateral TEXT,
    PubMsg TEXT
);

CREATE INDEX IF NOT EXISTS index_funds_tagged_deal_uuid on FundsTagged(DealUUID);

CREATE TABLE IF NOT EXISTS StorageLogs (
    DealUUID TEXT,
    CreatedAt DateTime,
    TransferSize TEXT,
    LogText TEXT
);

CREATE INDEX IF NOT EXISTS index_storage_logs_deal_uuid on StorageLogs(DealUUID);

CREATE TABLE IF NOT EXISTS StorageTagged (
    DealUUID TEXT,
    CreatedAt DateTime,
    TransferSize TEXT
);

CREATE INDEX IF NOT EXISTS index_storage_tagged_deal_uuid on StorageTagged(DealUUID);
