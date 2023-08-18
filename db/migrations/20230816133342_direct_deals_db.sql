-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS DirectDeals (
    ID TEXT,
    CreatedAt DateTime,
    PieceCID TEXT,
    PieceSize INT,
    ClientAddress TEXT,
    ProviderAddress TEXT,
    StartEpoch INT,
    EndEpoch INT,
    InboundFilePath TEXT,
    SectorID INT,
    Offset INT,
    Length INT,
    Error TEXT,
    PRIMARY KEY(ID)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE DirectDeals;
-- +goose StatementEnd
