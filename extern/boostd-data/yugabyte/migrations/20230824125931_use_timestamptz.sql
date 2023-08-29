-- +goose Up
-- +goose StatementBegin
ALTER TABLE PieceTracker ALTER CreatedAt TYPE timestamptz;
ALTER TABLE PieceTracker ALTER UpdatedAt TYPE timestamptz;
ALTER TABLE PieceFlagged ALTER CreatedAt TYPE timestamptz;
ALTER TABLE PieceFlagged ALTER UpdatedAt TYPE timestamptz;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE PieceTracker ALTER CreatedAt TYPE timestamp;
ALTER TABLE PieceTracker ALTER UpdatedAt TYPE timestamp;
ALTER TABLE PieceFlagged ALTER CreatedAt TYPE timestamp;
ALTER TABLE PieceFlagged ALTER UpdatedAt TYPE timestamp;
-- +goose StatementEnd
