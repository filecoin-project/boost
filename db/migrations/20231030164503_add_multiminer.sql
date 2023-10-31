-- +goose Up
-- +goose StatementBegin
ALTER TABLE ProposalLogs
  ADD ProviderAddress TEXT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE ProposalLogs
  DROP COLUMN ProviderAddress;
-- +goose StatementEnd
