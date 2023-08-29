-- +goose Up
-- +goose StatementBegin
-- Delete duplicate rows, where the piece cid is the same and the address
-- is the same but one row is a t address and the other is the f address.
-- (delete the row with the t address)
DELETE FROM PieceTracker pt1
USING PieceTracker pt2
    WHERE
        pt1.PieceCid = pt2.PieceCid AND
        substr(pt1.MinerAddr, 1, 1) = 't' AND
        substr(pt2.MinerAddr, 1, 1) != 't' AND
        substr(pt1.MinerAddr, 2) = substr(pt2.MinerAddr, 2)
;
UPDATE PieceTracker SET MinerAddr = 'f' || substr(MinerAddr, 2) WHERE substr(MinerAddr, 1, 1) = 't';

DELETE FROM PieceFlagged pt1
USING PieceFlagged pt2
WHERE
    pt1.PieceCid = pt2.PieceCid AND
    substr(pt1.MinerAddr, 1, 1) = 't' AND
    substr(pt2.MinerAddr, 1, 1) != 't' AND
    substr(pt1.MinerAddr, 2) = substr(pt2.MinerAddr, 2)
;
UPDATE PieceFlagged SET MinerAddr = 'f' || substr(MinerAddr, 2) WHERE substr(MinerAddr, 1, 1) = 't';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
