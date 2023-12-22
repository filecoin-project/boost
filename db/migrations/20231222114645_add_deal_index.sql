-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS index_deals_piece_cid on Deals(PieceCID);
CREATE INDEX IF NOT EXISTS index_deals_checkpoint on Deals(Checkpoint);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS index_deals_piece_cid;
DROP INDEX IF EXISTS index_deals_checkpoint;
-- +goose StatementEnd
