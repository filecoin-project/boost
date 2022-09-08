-- +goose Up
-- +goose StatementBegin
ALTER TABLE StorageTagged
    ADD TransferHost TEXT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
