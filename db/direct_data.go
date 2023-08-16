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
var directdataFields []string
var directdataFieldsStr = ""

func init() {
	var deal types.DirectDataEntry
	def := newdirectdataAccessor(nil, &deal)
	directdataFields = make([]string, 0, len(def.def))
	for k := range def.def {
		directdataFields = append(directdataFields, k)
	}
	directdataFieldsStr = strings.Join(directdataFields, ", ")
}

type directdataAccessor struct {
	db   *sql.DB
	deal *types.DirectDataEntry
	def  map[string]fielddef.FieldDefinition
}

func (d *DirectDataDB) newDirectDataDef(deal *types.DirectDataEntry) *directdataAccessor {
	return newdirectdataAccessor(d.db, deal)
}

func newdirectdataAccessor(db *sql.DB, deal *types.DirectDataEntry) *directdataAccessor {
	return &directdataAccessor{
		db:   db,
		deal: deal,
		def: map[string]fielddef.FieldDefinition{
			"ID":          &fielddef.FieldDef{F: &deal.ID},
			"CreatedAt":   &fielddef.FieldDef{F: &deal.CreatedAt},
			"PieceCID":    &fielddef.CidFieldDef{F: &deal.PieceCID},
			"PieceSize":   &fielddef.FieldDef{F: &deal.PieceSize},
			"CleanupData": &fielddef.FieldDef{F: &deal.CleanupData},
			//"ClientAddress":      &fielddef.AddrFieldDef{F: &deal.ClientDealProposal.Proposal.Client},
			//"ProviderAddress":    &fielddef.AddrFieldDef{F: &deal.ClientDealProposal.Proposal.Provider},
			"StartEpoch": &fielddef.FieldDef{F: &deal.StartEpoch},
			"EndEpoch":   &fielddef.FieldDef{F: &deal.EndEpoch},
			//"ProviderCollateral": &fielddef.BigIntFieldDef{F: &deal.ClientDealProposal.Proposal.ProviderCollateral},
			//"ClientCollateral":   &fielddef.BigIntFieldDef{F: &deal.ClientDealProposal.Proposal.ClientCollateral},
			"InboundFilePath": &fielddef.FieldDef{F: &deal.InboundFilePath},
			"SectorID":        &fielddef.FieldDef{F: &deal.SectorID},
			"Offset":          &fielddef.FieldDef{F: &deal.Offset},
			"Length":          &fielddef.FieldDef{F: &deal.Length},
			"Error":           &fielddef.FieldDef{F: &deal.Err},
			"Retry":           &fielddef.FieldDef{F: &deal.Retry},
			"FastRetrieval":   &fielddef.FieldDef{F: &deal.FastRetrieval},
			"AnnounceToIPNI":  &fielddef.FieldDef{F: &deal.AnnounceToIPNI},
		},
	}
}

func (d *directdataAccessor) scan(row Scannable) error {
	// For each field
	dest := []interface{}{}
	for _, name := range directdataFields {
		// Get a pointer to the field that will receive the scanned value
		fieldDef := d.def[name]
		dest = append(dest, fieldDef.FieldPtr())
	}

	// Scan the row into each pointer
	err := row.Scan(dest...)
	if err != nil {
		return fmt.Errorf("scanning deal row: %w", err)
	}

	// For each field
	for name, fieldDef := range d.def {
		// Unmarshall the scanned value into deal object
		err := fieldDef.Unmarshall()
		if err != nil {
			return fmt.Errorf("unmarshalling db field %s: %s", name, err)
		}
	}
	return nil
}

func (d *directdataAccessor) insert(ctx context.Context) error {
	// For each field
	values := []interface{}{}
	placeholders := make([]string, 0, len(values))
	for _, name := range directdataFields {
		// Add a placeholder "?"
		fieldDef := d.def[name]
		placeholders = append(placeholders, "?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.Marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the INSERT
	qry := "INSERT INTO DirectData (" + directdataFieldsStr + ") "
	qry += "VALUES (" + strings.Join(placeholders, ",") + ")"
	_, err := d.db.ExecContext(ctx, qry, values...)
	return err
}

func (d *directdataAccessor) update(ctx context.Context) error {
	// For each field
	values := []interface{}{}
	setNames := make([]string, 0, len(values))
	for _, name := range directdataFields {
		// Skip the ID field
		if name == "ID" {
			continue
		}

		// Add "fieldName = ?"
		fieldDef := d.def[name]
		setNames = append(setNames, name+" = ?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.Marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the UPDATE
	qry := "UPDATE DirectData "
	qry += "SET " + strings.Join(setNames, ", ")

	qry += "WHERE ID = ?"
	values = append(values, d.deal.ID)

	_, err := d.db.ExecContext(ctx, qry, values...)
	return err
}

type DirectDataDB struct {
	db *sql.DB
}

func NewDirectDataDB(db *sql.DB) *DirectDataDB {
	return &DirectDataDB{db: db}
}

func (d *DirectDataDB) Insert(ctx context.Context, deal *types.DirectDataEntry) error {
	return d.newDirectDataDef(deal).insert(ctx)
}

func (d *DirectDataDB) Update(ctx context.Context, deal *types.DirectDataEntry) error {
	return d.newDirectDataDef(deal).update(ctx)
}

func (d *DirectDataDB) ByID(ctx context.Context, id uuid.UUID) (*types.DirectDataEntry, error) {
	qry := "SELECT " + directdataFieldsStr + " FROM DirectData WHERE id=?"
	row := d.db.QueryRowContext(ctx, qry, id)
	return d.scanRow(row)
}

func (d *DirectDataDB) ByPieceCID(ctx context.Context, pieceCid cid.Cid) ([]*types.DirectDataEntry, error) {
	return d.list(ctx, 0, 0, "PieceCID=?", pieceCid.String())
}

func (d *DirectDataDB) BySectorID(ctx context.Context, sectorID abi.SectorID) ([]*types.DirectDataEntry, error) {
	addr, err := address.NewIDAddress(uint64(sectorID.Miner))
	if err != nil {
		return nil, fmt.Errorf("creating address from ID %d: %w", sectorID.Miner, err)
	}

	return d.list(ctx, 0, 0, "ProviderAddress=? AND SectorID=?", addr.String(), sectorID.Number)
}

func (d *DirectDataDB) Count(ctx context.Context, query string, filter *FilterOptions) (int, error) {
	whereArgs := []interface{}{}
	where := "SELECT count(*) FROM DirectData"
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

func (d *DirectDataDB) ListActive(ctx context.Context) ([]*types.DirectDataEntry, error) {
	return d.list(ctx, 0, 0, "Checkpoint != ?", dealcheckpoints.Complete.String())
}

func (d *DirectDataDB) ListCompleted(ctx context.Context) ([]*types.DirectDataEntry, error) {
	return d.list(ctx, 0, 0, "Checkpoint = ?", dealcheckpoints.Complete.String())
}

func (d *DirectDataDB) List(ctx context.Context, query string, filter *FilterOptions, cursor *graphql.ID, offset int, limit int) ([]*types.DirectDataEntry, error) {
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

func (d *DirectDataDB) list(ctx context.Context, offset int, limit int, whereClause string, whereArgs ...interface{}) ([]*types.DirectDataEntry, error) {
	args := whereArgs
	qry := "SELECT " + directdataFieldsStr + " FROM DirectData"
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

	deals := make([]*types.DirectDataEntry, 0, 16)
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

func (d *DirectDataDB) scanRow(row Scannable) (*types.DirectDataEntry, error) {
	var deal types.DirectDataEntry
	err := d.newDirectDataDef(&deal).scan(row)
	return &deal, err
}
