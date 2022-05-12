-- +goose Up
-- +goose StatementBegin
ALTER TABLE Deals
    ADD Retry TEXT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
