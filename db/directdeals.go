package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/filecoin-project/boost/db/fielddef"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
	_ "github.com/mattn/go-sqlite3"
)

// Used for SELECT statements: "ID, CreatedAt, ..."
var directDealsFields []string
var directDealsFieldsStr = ""

func init() {
	var deal types.DirectDeal
	def := newDirectDealsAccessor(nil, &deal)
	directDealsFields = make([]string, 0, len(def.def))
	for k := range def.def {
		directDealsFields = append(directDealsFields, k)
	}
	directDealsFieldsStr = strings.Join(directDealsFields, ", ")
}

type directDealsAccessor struct {
	db   *sql.DB
	deal *types.DirectDeal
	def  map[string]fielddef.FieldDefinition
}

func (d *DirectDataDB) newDirectDataDef(deal *types.DirectDeal) *directDealsAccessor {
	return newDirectDealsAccessor(d.db, deal)
}

func newDirectDealsAccessor(db *sql.DB, deal *types.DirectDeal) *directDealsAccessor {
	return &directDealsAccessor{
		db:   db,
		deal: deal,
		def: map[string]fielddef.FieldDefinition{
			"ID":              &fielddef.FieldDef{F: &deal.ID},
			"CreatedAt":       &fielddef.FieldDef{F: &deal.CreatedAt},
			"PieceCID":        &fielddef.CidFieldDef{F: &deal.PieceCID},
			"PieceSize":       &fielddef.FieldDef{F: &deal.PieceSize},
			"CleanupData":     &fielddef.FieldDef{F: &deal.CleanupData},
			"ClientAddress":   &fielddef.AddrFieldDef{F: &deal.Client},
			"ProviderAddress": &fielddef.AddrFieldDef{F: &deal.Provider},
			"AllocationID":    &fielddef.FieldDef{F: &deal.AllocationID},
			"StartEpoch":      &fielddef.FieldDef{F: &deal.StartEpoch},
			"EndEpoch":        &fielddef.FieldDef{F: &deal.EndEpoch},
			//"ProviderCollateral": &fielddef.BigIntFieldDef{F: &deal.ClientDealProposal.Proposal.ProviderCollateral},
			//"ClientCollateral":   &fielddef.BigIntFieldDef{F: &deal.ClientDealProposal.Proposal.ClientCollateral},
			"InboundFilePath":  &fielddef.FieldDef{F: &deal.InboundFilePath},
			"SectorID":         &fielddef.FieldDef{F: &deal.SectorID},
			"Offset":           &fielddef.FieldDef{F: &deal.Offset},
			"Length":           &fielddef.FieldDef{F: &deal.Length},
			"Checkpoint":       &fielddef.CkptFieldDef{F: &deal.Checkpoint},
			"CheckpointAt":     &fielddef.FieldDef{F: &deal.CheckpointAt},
			"Error":            &fielddef.FieldDef{F: &deal.Err},
			"Retry":            &fielddef.FieldDef{F: &deal.Retry},
			"KeepUnsealedCopy": &fielddef.FieldDef{F: &deal.KeepUnsealedCopy},
			"AnnounceToIPNI":   &fielddef.FieldDef{F: &deal.AnnounceToIPNI},
		},
	}
}

func (d *directDealsAccessor) scan(row Scannable) error {
	return scan(directDealsFields, d.def, row)
}

func (d *directDealsAccessor) insert(ctx context.Context) error {
	return insert(ctx, "DirectDeals", directDealsFields, directDealsFieldsStr, d.def, d.db)
}

func (d *directDealsAccessor) update(ctx context.Context) error {
	return update(ctx, "DirectDeals", directDealsFields, d.def, d.db, d.deal.ID)
}

type DirectDataDB struct {
	db *sql.DB
}

func NewDirectDataDB(db *sql.DB) *DirectDataDB {
	return &DirectDataDB{db: db}
}

func (d *DirectDataDB) Insert(ctx context.Context, deal *types.DirectDeal) error {
	return d.newDirectDataDef(deal).insert(ctx)
}

func (d *DirectDataDB) Update(ctx context.Context, deal *types.DirectDeal) error {
	return d.newDirectDataDef(deal).update(ctx)
}

func (d *DirectDataDB) ByID(ctx context.Context, id uuid.UUID) (*types.DirectDeal, error) {
	qry := "SELECT " + directDealsFieldsStr + " FROM DirectDeals WHERE ID=?"
	row := d.db.QueryRowContext(ctx, qry, id)
	return d.scanRow(row)
}

func (d *DirectDataDB) ByPieceCID(ctx context.Context, pieceCid cid.Cid) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "PieceCID=?", pieceCid.String())
}

func (d *DirectDataDB) BySectorID(ctx context.Context, sectorID abi.SectorID) ([]*types.DirectDeal, error) {
	addr, err := address.NewIDAddress(uint64(sectorID.Miner))
	if err != nil {
		return nil, fmt.Errorf("creating address from ID %d: %w", sectorID.Miner, err)
	}

	return d.list(ctx, 0, 0, "ProviderAddress=? AND SectorID=?", addr.String(), sectorID.Number)
}

func (d *DirectDataDB) ListAll(ctx context.Context) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "")
}

func (d *DirectDataDB) Count(ctx context.Context, query string, filter *FilterOptions) (int, error) {
	whereArgs := []interface{}{}
	where := "SELECT count(*) FROM DirectDeals"
	if query != "" {
		searchWhere, searchArgs := withSearchQuery(query)
		where += " WHERE " + searchWhere
		whereArgs = append(whereArgs, searchArgs...)
	}

	if filter != nil {
		filterWhere, filterArgs := withSearchFilter(*filter)

		if filterWhere != "" {
			if query != "" {
				where += " AND "
			} else {
				where += " WHERE "
			}
			where += filterWhere
			whereArgs = append(whereArgs, filterArgs...)
		}
	}
	row := d.db.QueryRowContext(ctx, where, whereArgs...)

	var count int
	err := row.Scan(&count)
	return count, err
}

func (d *DirectDataDB) List(ctx context.Context, query string, filter *FilterOptions, cursor *graphql.ID, offset int, limit int) ([]*types.DirectDeal, error) {
	where := ""
	whereArgs := []interface{}{}

	// Add pagination parameters
	if cursor != nil {
		where += "CreatedAt <= (SELECT CreatedAt FROM Deals WHERE ID = ?)"
		whereArgs = append(whereArgs, *cursor)
	}

	// Add search query parameters
	if query != "" {
		if where != "" {
			where += " AND "
		}
		searchWhere, searchArgs := withSearchQuery(query)
		where += searchWhere
		whereArgs = append(whereArgs, searchArgs...)
	}

	if filter != nil {
		if where != "" {
			where += " AND "
		}

		filterWhere, filterArgs := withSearchFilter(*filter)
		if filterWhere != "" {
			where += filterWhere
			whereArgs = append(whereArgs, filterArgs...)
		}
	}

	return d.list(ctx, offset, limit, where, whereArgs...)
}

func (d *DirectDataDB) ListActive(ctx context.Context) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "Checkpoint != ?", dealcheckpoints.Complete.String())
}

func (d *DirectDataDB) ListCompleted(ctx context.Context) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "Checkpoint = ?", dealcheckpoints.Complete.String())
}

func (d *DirectDataDB) list(ctx context.Context, offset int, limit int, whereClause string, whereArgs ...interface{}) ([]*types.DirectDeal, error) {
	args := whereArgs
	qry := "SELECT " + directDealsFieldsStr + " FROM DirectDeals"
	if whereClause != "" {
		qry += " WHERE " + whereClause
	}
	qry += " ORDER BY CreatedAt DESC"
	if limit > 0 {
		qry += " LIMIT ?"
		args = append(args, limit)

		if offset > 0 {
			qry += " OFFSET ?"
			args = append(args, offset)
		}
	}

	rows, err := d.db.QueryContext(ctx, qry, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deals := make([]*types.DirectDeal, 0, 16)
	for rows.Next() {
		deal, err := d.scanRow(rows)
		if err != nil {
			return nil, err
		}
		deals = append(deals, deal)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return deals, nil
}

func (d *DirectDataDB) scanRow(row Scannable) (*types.DirectDeal, error) {
	var deal types.DirectDeal
	err := d.newDirectDataDef(&deal).scan(row)
	return &deal, err
}
