CREATE TABLE IF NOT EXISTS PieceTracker (
    PieceCid TEXT PRIMARY KEY,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
);

CREATE INDEX IF NOT EXISTS PieceTrackerCreatedAt ON PieceTracker (CreatedAt);
CREATE INDEX IF NOT EXISTS PieceTrackerUpdatedAt ON PieceTracker (UpdatedAt);

CREATE TABLE IF NOT EXISTS PieceFlagged (
    PieceCid TEXT PRIMARY KEY,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP,
    HasUnsealedCopy BOOLEAN
);

CREATE INDEX IF NOT EXISTS PieceFlaggedCreatedAt ON PieceFlagged (CreatedAt);
CREATE INDEX IF NOT EXISTS PieceFlaggedUpdatedAt ON PieceFlagged (UpdatedAt);
