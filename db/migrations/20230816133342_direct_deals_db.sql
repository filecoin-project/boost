-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS DirectData (
    ID TEXT,
    CreatedAt DateTime,
    PieceCID TEXT,
    PieceSize INT,
    VerifiedDeal BOOL,
    ClientAddress TEXT,
    ProviderAddress TEXT,
    StartEpoch INT,
    EndEpoch INT,
    InboundFilePath TEXT,
    TransferType TEXT,
    TransferParams BLOB,
    TransferSize INT,
    SectorID INT,
    Offset INT,
    Length INT,
    Error TEXT,
    PRIMARY KEY(ID)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE DirectData;
-- +goose StatementEnd
