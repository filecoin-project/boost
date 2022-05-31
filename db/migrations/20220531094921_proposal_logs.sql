-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ProposalLogs (
    DealUUID TEXT,
    Accepted BOOL,
    Reason TEXT,
    CreatedAt DateTime,
    ClientAddress TEXT,
    PieceSize INT
);

CREATE INDEX IF NOT EXISTS index_proposallogs_deal_uuid on ProposalLogs(DealUUID);
CREATE INDEX IF NOT EXISTS index_proposallogs_created_at on ProposalLogs(CreatedAt);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE ProposalLogs;
-- +goose StatementEnd
