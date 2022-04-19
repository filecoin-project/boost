-- +goose Up
-- +goose StatementBegin
ALTER TABLE Deals
  ADD CheckpointAt DateTime;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
