CREATE TABLE PieceTracker (
    PieceCid TEXT PRIMARY KEY,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
);

CREATE INDEX PieceTrackerCreatedAt ON PieceTracker (CreatedAt);
CREATE INDEX PieceTrackerUpdatedAt ON PieceTracker (UpdatedAt);

CREATE TABLE PieceFlagged (
    PieceCid TEXT PRIMARY KEY,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
);

CREATE INDEX PieceFlaggedCreatedAt ON PieceFlagged (CreatedAt);
CREATE INDEX PieceFlaggedUpdatedAt ON PieceFlagged (UpdatedAt);
