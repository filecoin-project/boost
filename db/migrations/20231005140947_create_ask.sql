-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS StorageAsk (
                                           Price INT,
                                           VerifiedPrice INT,
                                           MinPieceSize INT,
                                           MaxPieceSize INT,
                                           Miner        Text,
                                           TS    INT,
                                           Expiry       INT,
                                           SeqNo        INT
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS StorageAsk;
-- +goose StatementEnd