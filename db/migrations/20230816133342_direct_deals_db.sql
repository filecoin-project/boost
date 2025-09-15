-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS DirectDeals (
    ID TEXT,
    CreatedAt DateTime,
    PieceCID TEXT,
    PieceSize INT,
    CleanupData BOOL,
    ClientAddress TEXT,
    ProviderAddress TEXT,
    AllocationID INT,
    StartEpoch INT,
    EndEpoch INT,
    InboundFilePath TEXT,
    InboundFileSize INT,
    SectorID INT,
    Offset INT,
    Length INT,
    Checkpoint TEXT,
    CheckpointAt DateTime,
    Error TEXT,
    Retry TEXT,
    AnnounceToIPNI BOOL,
    KeepUnsealedCopy BOOL,
    Notifications TEXT,
    PRIMARY KEY(ID)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE DirectDeals;
-- +goose StatementEnd
