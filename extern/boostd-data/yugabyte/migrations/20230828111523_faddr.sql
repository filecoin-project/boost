-- +goose Up
-- +goose StatementBegin
UPDATE PieceTracker SET MinerAddr = 'f' || substr(MinerAddr, 2) WHERE substr(MinerAddr, 1, 1) = 't';
UPDATE PieceFlagged SET MinerAddr = 'f' || substr(MinerAddr, 2) WHERE substr(MinerAddr, 1, 1) = 't';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
