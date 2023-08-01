package yugabyte

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

var TrackerCheckBatchSize = 1024

const insertTrackerParallelism = 16

type pieceCreated struct {
	MinerAddr address.Address
	PieceCid  cid.Cid
	CreatedAt time.Time
}

// NextPiecesToCheck is periodically called by the piece doctor.
// It returns a selection of piece cids so the piece doctor can check the
// status of each piece.
// For each piece it saves the time at which the piece was checked, so that
// the piece won't be checked again for a while.
// The implementation uses a PieceTracker table to keep track of when each piece
// was last checked.
func (s *Store) NextPiecesToCheck(ctx context.Context, maddr address.Address) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.next_pieces_to_check")
	defer span.End()

	// Get the time at which pieces were last copied from the piece metadata
	// to the piece tracker table
	var lastCopiedRes pgtype.Timestamptz
	err := s.db.QueryRow(ctx, `SELECT MAX(CreatedAt) FROM PieceTracker`).Scan(&lastCopiedRes)
	if err != nil {
		return nil, fmt.Errorf("getting time piece tracker was last updated: %w", err)
	}

	lastCopied := lastCopiedRes.Time
	if lastCopiedRes.Status&pgtype.Present == 0 {
		// If there are no results, set last updated to the zero value of time,
		// so that we copy across all rows from piece metadata to piece tracker
		lastCopied = time.UnixMilli(0)
	}
	log.Debugw("got tracker last updated", "updated at", lastCopied.String())

	// Get the list of pieces that have been added since tracking information
	// was last updated
	qry := `SELECT PieceCid, CreatedAt from idx.PieceMetadata WHERE CreatedAt >= ?`
	iter := s.session.Query(qry, lastCopied).WithContext(ctx).Iter()
	var newPieces []pieceCreated
	var createdAt time.Time
	var pcidstr string
	for iter.Scan(&pcidstr, &createdAt) {
		c, err := cid.Parse(pcidstr)
		if err != nil {
			return nil, fmt.Errorf("getting new pieces: parsing piece cid %s: %w", pcidstr, err)
		}
		newPieces = append(newPieces, pieceCreated{PieceCid: c, CreatedAt: createdAt})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("getting new pieces: %w", err)
	}

	// Get the miners on which the piece was stored
	log.Debugw("getting miners for pieces", "count", len(newPieces))
	var newPieceWithMaddrLk sync.Mutex
	var newPiecesWithMaddr []pieceCreated
	err = s.execWithConcurrency(ctx, newPieces, insertTrackerParallelism, func(pc pieceCreated) error {
		qry := `SELECT MinerAddr FROM idx.PieceDeal WHERE PieceCid = ?`
		iter := s.session.Query(qry, pc.PieceCid).WithContext(ctx).Iter()
		var maddrStr string
		for iter.Scan(&maddrStr) {
			maddr, err := address.NewFromString(maddrStr)
			if err != nil {
				return fmt.Errorf("getting new pieces: parsing miner adddress '%s': %w", maddrStr, err)
			}

			c, err := cid.Parse(pcidstr)
			if err != nil {
				return fmt.Errorf("getting new pieces: parsing piece cid %s: %w", pcidstr, err)
			}
			newPieceWithMaddrLk.Lock()
			newPieces = append(newPieces, pieceCreated{MinerAddr: maddr, PieceCid: c, CreatedAt: createdAt})
			newPieceWithMaddrLk.Unlock()
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("getting new pieces: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Add any new pieces into the piece status tracking table
	log.Debugw("inserting new pieces into tracker", "count", len(newPiecesWithMaddr))
	err = s.execWithConcurrency(ctx, newPiecesWithMaddr, insertTrackerParallelism, func(pc pieceCreated) error {
		qry := `INSERT INTO PieceTracker (MinerAddr, PieceCid, CreatedAt, UpdatedAt) ` +
			`VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`
		_, err := s.db.Exec(ctx, qry, pc.MinerAddr.String(), pc.PieceCid.String(), pc.CreatedAt, time.UnixMilli(0))
		if err != nil {
			return fmt.Errorf("inserting row into piece tracker: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Work out how frequently to check each piece, based on how many pieces
	// there are.
	// Any pieces that have not been checked in the last pieceCheckPeriod
	// will be checked now (eg check all pieces that haven't been checked
	// for 10s)
	pieceCheckPeriod, err := s.getPieceCheckPeriod(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting piece check period: %w", err)
	}

	// Get all pieces from the piece tracker table that have not been updated
	// since the last piece check period.
	// At the same time set the UpdatedAt field so that these pieces are marked
	// as checked (and will not be returned until the next piece check period
	// elapses again).
	// Note that we limit the number of rows to fetch so as not to overload the
	// system. Any rows beyond the limit will be fetched the next time
	// NextPiecesToCheck is called.
	now := time.Now()
	qry = `WITH cte AS (` +
		`SELECT PieceCid FROM PieceTracker WHERE MinerAddr = $1 AND UpdatedAt < $2 LIMIT $3` +
		`)` +
		`UPDATE PieceTracker pt SET UpdatedAt = $4 ` +
		`FROM cte WHERE pt.MinerAddr = cte.MinerAddr AND pt.PieceCid = cte.PieceCid ` +
		`RETURNING pt.PieceCid`
	rows, err := s.db.Query(ctx, qry, maddr.String(), now.Add(-pieceCheckPeriod), TrackerCheckBatchSize, now)
	if err != nil {
		return nil, fmt.Errorf("getting pieces from piece tracker: %w", err)
	}
	defer rows.Close()

	pcids := make([]cid.Cid, 0, TrackerCheckBatchSize)
	var pcid string
	for rows.Next() {
		err := rows.Scan(&pcid)
		if err != nil {
			return nil, fmt.Errorf("scanning piece tracker row: %w", err)
		}

		c, err := cid.Parse(pcid)
		if err != nil {
			return nil, fmt.Errorf("parsing tracker piece cid %s as cid: %w", pcid, err)
		}

		pcids = append(pcids, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting pieces to check: %w", err)
	}

	log.Debugw("got tracker pieces", "count", len(pcids),
		"last updated", lastCopied.String(), "now", now.String(), "piece-check-period", pieceCheckPeriod.String())
	return pcids, nil
}

func (s *Store) execWithConcurrency(ctx context.Context, pcids []pieceCreated, concurrency int, exec func(created pieceCreated) error) error {
	queue := make(chan pieceCreated, len(pcids))
	for _, pc := range pcids {
		queue <- pc
	}
	close(queue)

	var eg errgroup.Group
	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pc, ok := <-queue:
					if !ok {
						// Finished adding all the queued items, exit the thread
						return nil
					}

					err := exec(pc)
					if err != nil {
						return err
					}
				}
			}

			return ctx.Err()
		})
	}
	return eg.Wait()
}

// The minimum frequency with which to check pieces for errors (eg bad index)
var MinPieceCheckPeriod = 5 * time.Minute

// Work out how frequently to check each piece, based on how many pieces
// there are: if there are many pieces, each piece will be checked
// less frequently
func (s *Store) getPieceCheckPeriod(ctx context.Context) (time.Duration, error) {
	var count int
	err := s.db.QueryRow(ctx, `SELECT Count(*) FROM PieceTracker`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("getting count of rows in piece tracker: %w", err)
	}

	// Check period:
	// - 1k pieces;   every 100s (5 minutes because of MinPieceCheckPeriod)
	// - 100k pieces; every 150m
	// - 1m pieces;   every 20 hours
	period := time.Duration(count*100) * time.Millisecond
	if period < MinPieceCheckPeriod {
		period = MinPieceCheckPeriod
	}

	return period, nil
}

func (s *Store) FlagPiece(ctx context.Context, pieceCid cid.Cid, hasUnsealedCopy bool, maddr address.Address) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.flag_piece")
	span.SetAttributes(attribute.String("pieceCid", pieceCid.String()))
	defer span.End()

	now := time.Now()
	qry := `INSERT INTO PieceFlagged (MinerAddr, PieceCid, CreatedAt, UpdatedAt, HasUnsealedCopy) ` +
		`VALUES ($1, $2, $3, $4, $5) ` +
		`ON CONFLICT (MinerAddr, PieceCid) DO UPDATE SET UpdatedAt = excluded.UpdatedAt`
	_, err := s.db.Exec(ctx, qry, maddr.String(), pieceCid.String(), now, now, hasUnsealedCopy)
	if err != nil {
		return fmt.Errorf("flagging piece %s: %w", pieceCid, err)
	}
	return nil
}

func (s *Store) UnflagPiece(ctx context.Context, pieceCid cid.Cid, maddr address.Address) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.unflag_piece")
	span.SetAttributes(attribute.String("pieceCid", pieceCid.String()))
	defer span.End()

	qry := `DELETE FROM PieceFlagged WHERE MinerAddr = $1 AND PieceCid = $2`
	_, err := s.db.Exec(ctx, qry, maddr.String(), pieceCid.String())
	if err != nil {
		return fmt.Errorf("unflagging piece %s %s: %w", maddr, pieceCid, err)
	}

	return nil
}

func (s *Store) FlaggedPiecesList(ctx context.Context, filter *types.FlaggedPiecesListFilter, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.flagged_pieces")
	var spanCursor int
	if cursor != nil {
		spanCursor = int(cursor.UnixMilli())
	}
	span.SetAttributes(attribute.Int("cursor", spanCursor))
	span.SetAttributes(attribute.Int("offset", offset))
	span.SetAttributes(attribute.Int("limit", limit))
	defer span.End()

	var args []interface{}
	idx := 0
	qry := `SELECT MinerAddr, PieceCid, CreatedAt, UpdatedAt, HasUnsealedCopy from PieceFlagged `
	where := ""
	if cursor != nil {
		where += `WHERE CreatedAt < $1 `
		args = append(args, cursor)
		idx++
	}
	if filter != nil {
		if where == "" {
			where += `WHERE `
		} else {
			where += `AND `
		}
		where += fmt.Sprintf(`MinerAddr = $%d AND `, idx+1)
		args = append(args, filter.MinerAddr.String())

		where += fmt.Sprintf(`HasUnsealedCopy = $%d `, idx+1)
		args = append(args, filter.HasUnsealedCopy)
		idx++
	}
	qry += where
	qry += `ORDER BY CreatedAt desc `

	qry += fmt.Sprintf(`LIMIT $%d OFFSET $%d`, idx+1, idx+2)
	args = append(args, limit, offset)

	rows, err := s.db.Query(ctx, qry, args...)
	if err != nil {
		return nil, fmt.Errorf("getting flagged pieces: %w", err)
	}
	defer rows.Close()

	var pieces []model.FlaggedPiece
	var maddr string
	var pcid string
	var createdAt time.Time
	var updatedAt time.Time
	var hasUnsealedCopy bool
	for rows.Next() {
		err := rows.Scan(&maddr, &pcid, &createdAt, &updatedAt, &hasUnsealedCopy)
		if err != nil {
			return nil, fmt.Errorf("scanning flagged piece: %w", err)
		}

		ma, err := address.NewFromString(maddr)
		if err != nil {
			return nil, fmt.Errorf("parsing flagged piece miner address '%s': %w", maddr, err)
		}

		c, err := cid.Parse(pcid)
		if err != nil {
			return nil, fmt.Errorf("parsing flagged piece cid %s: %w", pcid, err)
		}

		pieces = append(pieces, model.FlaggedPiece{
			MinerAddr:       ma,
			PieceCid:        c,
			CreatedAt:       createdAt,
			UpdatedAt:       updatedAt,
			HasUnsealedCopy: hasUnsealedCopy,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting new pieces: %w", err)
	}

	return pieces, nil
}

func (s *Store) FlaggedPiecesCount(ctx context.Context, filter *types.FlaggedPiecesListFilter) (int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.flagged_pieces_count")
	defer span.End()

	var args []interface{}
	var count int
	qry := `SELECT COUNT(*) FROM PieceFlagged`
	if filter != nil {
		qry += ` WHERE MinerAddr = $1 AND HasUnsealedCopy = $2`
		args = append(args, filter.MinerAddr)
		args = append(args, filter.HasUnsealedCopy)
	}

	err := s.db.QueryRow(ctx, qry, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("getting flagged pieces count: %w", err)
	}

	return count, nil
}
