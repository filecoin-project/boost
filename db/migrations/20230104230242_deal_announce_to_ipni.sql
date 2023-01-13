-- +goose Up
-- +goose StatementBegin
ALTER TABLE Deals
    ADD AnnounceToIPNI BOOL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd