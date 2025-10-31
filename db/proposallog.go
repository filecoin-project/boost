package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/db/fielddef"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
)

type ProposalLog struct {
	DealUUID      uuid.UUID
	Accepted      bool
	Reason        string
	CreatedAt     time.Time
	ClientAddress address.Address
	PieceSize     abi.PaddedPieceSize
}

type ProposalLogsDB struct {
	db *sql.DB
}

func NewProposalLogsDB(db *sql.DB) *ProposalLogsDB {
	return &ProposalLogsDB{db: db}
}

func (p *ProposalLogsDB) InsertLog(ctx context.Context, deal types.DealParams, accepted bool, reason string) error {
	qry := "INSERT INTO ProposalLogs (DealUUID, Accepted, Reason, CreatedAt, ClientAddress, PieceSize) "
	qry += "VALUES (?, ?, ?, ?, ?, ?)"
	values := []interface{}{
		deal.DealUUID,
		accepted,
		reason,
		time.Now(),
		deal.ClientDealProposal.Proposal.Client.String(),
		deal.ClientDealProposal.Proposal.PieceSize,
	}
	_, err := p.db.ExecContext(ctx, qry, values...)
	return err
}

func (p *ProposalLogsDB) List(ctx context.Context, accepted *bool, cursor *time.Time, offset int, limit int) ([]ProposalLog, error) {
	qry := "SELECT DealUUID, Accepted, Reason, CreatedAt, ClientAddress, PieceSize FROM ProposalLogs"
	where := ""
	args := []interface{}{}
	if cursor != nil {
		where += " WHERE CreatedAt <= ?"
		args = append(args, cursor.Format(sqlite3.SQLiteTimestampFormats[0]))
	}
	if accepted != nil {
		if where == "" {
			where += " WHERE "
		} else {
			where += " AND "
		}
		where += "Accepted = ?"
		args = append(args, *accepted)
	}

	qry += where
	qry += " ORDER BY CreatedAt DESC, RowID"

	if limit > 0 {
		qry += " LIMIT ?"
		args = append(args, limit)

		if offset > 0 {
			qry += " OFFSET ?"
			args = append(args, offset)
		}
	}

	rows, err := p.db.QueryContext(ctx, qry, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	sz := limit
	if sz == 0 {
		sz = 16
	}
	propsLogs := make([]ProposalLog, 0, sz)
	for rows.Next() {
		var propsLog ProposalLog
		addrFD := &fielddef.AddrFieldDef{F: &propsLog.ClientAddress}
		err := rows.Scan(
			&propsLog.DealUUID,
			&propsLog.Accepted,
			&propsLog.Reason,
			&propsLog.CreatedAt,
			addrFD.FieldPtr(),
			&propsLog.PieceSize)
		if err != nil {
			return nil, fmt.Errorf("getting deal proposal log: %w", err)
		}

		err = addrFD.Unmarshall()
		if err != nil {
			return nil, fmt.Errorf("unmarshalling client address %s: %w", addrFD.Marshalled, err)
		}
		propsLog.ClientAddress = *addrFD.F

		propsLogs = append(propsLogs, propsLog)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return propsLogs, nil
}

func (p *ProposalLogsDB) Count(ctx context.Context, accepted *bool) (int, error) {
	var count int
	qry := "SELECT count(*) FROM ProposalLogs "
	var row *sql.Row
	if accepted == nil {
		row = p.db.QueryRowContext(ctx, qry)
	} else {
		row = p.db.QueryRowContext(ctx, qry+"WHERE Accepted=?", *accepted)
	}
	err := row.Scan(&count)
	return count, err
}

func (p *ProposalLogsDB) DeleteOlderThan(ctx context.Context, at time.Time) (int64, error) {
	res, err := p.db.ExecContext(ctx, "DELETE FROM ProposalLogs WHERE CreatedAt < ?", at)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
