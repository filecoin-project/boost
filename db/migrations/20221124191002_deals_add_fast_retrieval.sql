-- +goose Up
-- +goose StatementBegin
ALTER TABLE Deals
    ADD FastRetrieval BOOL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd