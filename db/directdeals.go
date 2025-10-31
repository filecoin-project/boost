package db

import (
	"context"
	"database/sql"
	"strings"

	"github.com/filecoin-project/boost/db/fielddef"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
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

func (d *DirectDealsDB) newDirectDataDef(deal *types.DirectDeal) *directDealsAccessor {
	return newDirectDealsAccessor(d.db, deal)
}

func newDirectDealsAccessor(db *sql.DB, deal *types.DirectDeal) *directDealsAccessor {
	return &directDealsAccessor{
		db:   db,
		deal: deal,
		def: map[string]fielddef.FieldDefinition{
			"ID":               &fielddef.FieldDef{F: &deal.ID},
			"CreatedAt":        &fielddef.FieldDef{F: &deal.CreatedAt},
			"PieceCID":         &fielddef.CidFieldDef{F: &deal.PieceCID},
			"PieceSize":        &fielddef.FieldDef{F: &deal.PieceSize},
			"CleanupData":      &fielddef.FieldDef{F: &deal.CleanupData},
			"ClientAddress":    &fielddef.AddrFieldDef{F: &deal.Client},
			"ProviderAddress":  &fielddef.AddrFieldDef{F: &deal.Provider},
			"AllocationID":     &fielddef.FieldDef{F: &deal.AllocationID},
			"StartEpoch":       &fielddef.FieldDef{F: &deal.StartEpoch},
			"EndEpoch":         &fielddef.FieldDef{F: &deal.EndEpoch},
			"InboundFilePath":  &fielddef.FieldDef{F: &deal.InboundFilePath},
			"InboundFileSize":  &fielddef.FieldDef{F: &deal.InboundFileSize},
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

type DirectDealsDB struct {
	db *sql.DB
}

func NewDirectDealsDB(db *sql.DB) *DirectDealsDB {
	return &DirectDealsDB{db: db}
}

func (d *DirectDealsDB) Insert(ctx context.Context, deal *types.DirectDeal) error {
	return d.newDirectDataDef(deal).insert(ctx)
}

func (d *DirectDealsDB) Update(ctx context.Context, deal *types.DirectDeal) error {
	return d.newDirectDataDef(deal).update(ctx)
}

func (d *DirectDealsDB) ByID(ctx context.Context, id uuid.UUID) (*types.DirectDeal, error) {
	qry := "SELECT " + directDealsFieldsStr + " FROM DirectDeals WHERE ID=?"
	row := d.db.QueryRowContext(ctx, qry, id)
	return d.scanRow(row)
}

func (d *DirectDealsDB) ByPieceCID(ctx context.Context, pieceCid cid.Cid) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "PieceCID=?", pieceCid.String())
}

func (d *DirectDealsDB) BySectorID(ctx context.Context, sectorID abi.SectorNumber) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "SectorID=?", sectorID)
}

func (d *DirectDealsDB) ActiveByPieceAllocID(ctx context.Context, piece cid.Cid, alloc verifreg.AllocationId) ([]*types.DirectDeal, error) {
	whereArgs := []interface{}{}
	whereArgs = append(whereArgs, piece.String())
	whereArgs = append(whereArgs, uint64(alloc))
	whereArgs = append(whereArgs, dealcheckpoints.Complete.String())
	return d.list(ctx, 0, 0, "PieceCID=? AND AllocationID=? AND Checkpoint != ?", whereArgs...)
}

func (d *DirectDealsDB) ListAll(ctx context.Context) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "")
}

var ddSearchFields = []string{"ID", "PieceCID", "ClientAddress", "ProviderAddress"}

func (d *DirectDealsDB) Count(ctx context.Context, query string, filter *FilterOptions) (int, error) {
	whereArgs := []interface{}{}
	where := "SELECT count(*) FROM DirectDeals"
	if query != "" {
		searchWhere, searchArgs := withSearchQuery(ddSearchFields, query, false)
		where += " WHERE " + searchWhere
		whereArgs = append(whereArgs, searchArgs...)
	}

	if filter != nil {
		filterWhere, filterArgs := withDirectDealsSearchFilter(*filter)

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

func (d *DirectDealsDB) List(ctx context.Context, query string, filter *FilterOptions, cursor *graphql.ID, offset int, limit int) ([]*types.DirectDeal, error) {
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
		searchWhere, searchArgs := withSearchQuery(ddSearchFields, query, false)
		where += searchWhere
		whereArgs = append(whereArgs, searchArgs...)
	}

	if filter != nil {
		if where != "" {
			where += " AND "
		}

		filterWhere, filterArgs := withDirectDealsSearchFilter(*filter)
		if filterWhere != "" {
			where += filterWhere
			whereArgs = append(whereArgs, filterArgs...)
		}
	}

	return d.list(ctx, offset, limit, where, whereArgs...)
}

func withDirectDealsSearchFilter(filter FilterOptions) (string, []interface{}) {
	whereArgs := []interface{}{}
	statements := []string{}

	if filter.Checkpoint != nil {
		statements = append(statements, "Checkpoint = ?")
		whereArgs = append(whereArgs, *filter.Checkpoint)
	}

	if len(statements) == 0 {
		return "", whereArgs
	}

	where := "(" + strings.Join(statements, " AND ") + ")"
	return where, whereArgs
}

func (d *DirectDealsDB) ListActive(ctx context.Context) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "Checkpoint != ?", dealcheckpoints.Complete.String())
}

func (d *DirectDealsDB) ListCompleted(ctx context.Context) ([]*types.DirectDeal, error) {
	return d.list(ctx, 0, 0, "Checkpoint = ?", dealcheckpoints.Complete.String())
}

func (d *DirectDealsDB) list(ctx context.Context, offset int, limit int, whereClause string, whereArgs ...interface{}) ([]*types.DirectDeal, error) {
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
	defer func() {
		_ = rows.Close()
	}()

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

func (d *DirectDealsDB) scanRow(row Scannable) (*types.DirectDeal, error) {
	var deal types.DirectDeal
	err := d.newDirectDataDef(&deal).scan(row)
	return &deal, err
}
