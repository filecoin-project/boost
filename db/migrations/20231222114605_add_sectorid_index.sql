-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS index_deals_sector_id on Deals(SectorID);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS index_deals_sector_id;
-- +goose StatementEnd
