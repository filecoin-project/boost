-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS SectorState (
    MinerID INT,
    SectorID INT,
    UpdatedAt DateTime,
    SealState TEXT
);

CREATE INDEX IF NOT EXISTS index_sector_state_sector_id on SectorState(SectorID);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS index_sector_state_sector_id;
DROP TABLE IF EXISTS SectorState;
-- +goose StatementEnd
