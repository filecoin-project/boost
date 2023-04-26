-- +goose Up
-- +goose StatementBegin
DROP INDEX index_sector_state_sector_id;
DELETE FROM SectorState;
CREATE UNIQUE INDEX IF NOT EXISTS index_sector_state_sector_id on SectorState(MinerID,SectorID);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS index_sector_state_sector_id;
-- +goose StatementEnd
