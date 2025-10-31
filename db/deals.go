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
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
	_ "github.com/mattn/go-sqlite3"
)

// Used for SELECT statements: "ID, CreatedAt, ..."
var dealFields []string
var dealFieldsStr = ""

func init() {
	var deal types.ProviderDealState
	deal.ClientDealProposal = market.ClientDealProposal{
		Proposal: market.DealProposal{},
	}
	def := newDealAccessor(nil, &deal)
	dealFields = make([]string, 0, len(def.def))
	for k := range def.def {
		dealFields = append(dealFields, k)
	}
	dealFieldsStr = strings.Join(dealFields, ", ")
}

type dealAccessor struct {
	db   *sql.DB
	deal *types.ProviderDealState
	def  map[string]fielddef.FieldDefinition
}

type FilterOptions struct {
	Checkpoint   *string
	IsOffline    *bool
	TransferType *string
	IsVerified   *bool
}

func (d *DealsDB) newDealDef(deal *types.ProviderDealState) *dealAccessor {
	return newDealAccessor(d.db, deal)
}

func newDealAccessor(db *sql.DB, deal *types.ProviderDealState) *dealAccessor {
	return &dealAccessor{
		db:   db,
		deal: deal,
		def: map[string]fielddef.FieldDefinition{
			"ID":                    &fielddef.FieldDef{F: &deal.DealUuid},
			"CreatedAt":             &fielddef.FieldDef{F: &deal.CreatedAt},
			"DealProposalSignature": &fielddef.SigFieldDef{F: &deal.ClientDealProposal.ClientSignature},
			"PieceCID":              &fielddef.CidFieldDef{F: &deal.ClientDealProposal.Proposal.PieceCID},
			"PieceSize":             &fielddef.FieldDef{F: &deal.ClientDealProposal.Proposal.PieceSize},
			"VerifiedDeal":          &fielddef.FieldDef{F: &deal.ClientDealProposal.Proposal.VerifiedDeal},
			"IsOffline":             &fielddef.FieldDef{F: &deal.IsOffline},
			"CleanupData":           &fielddef.FieldDef{F: &deal.CleanupData},
			"ClientAddress":         &fielddef.AddrFieldDef{F: &deal.ClientDealProposal.Proposal.Client},
			"ProviderAddress":       &fielddef.AddrFieldDef{F: &deal.ClientDealProposal.Proposal.Provider},
			"Label":                 &fielddef.LabelFieldDef{F: &deal.ClientDealProposal.Proposal.Label},
			"StartEpoch":            &fielddef.FieldDef{F: &deal.ClientDealProposal.Proposal.StartEpoch},
			"EndEpoch":              &fielddef.FieldDef{F: &deal.ClientDealProposal.Proposal.EndEpoch},
			"StoragePricePerEpoch":  &fielddef.BigIntFieldDef{F: &deal.ClientDealProposal.Proposal.StoragePricePerEpoch},
			"ProviderCollateral":    &fielddef.BigIntFieldDef{F: &deal.ClientDealProposal.Proposal.ProviderCollateral},
			"ClientCollateral":      &fielddef.BigIntFieldDef{F: &deal.ClientDealProposal.Proposal.ClientCollateral},
			"ClientPeerID":          &fielddef.PeerIDFieldDef{F: &deal.ClientPeerID},
			"DealDataRoot":          &fielddef.CidFieldDef{F: &deal.DealDataRoot},
			"InboundFilePath":       &fielddef.FieldDef{F: &deal.InboundFilePath},
			"TransferType":          &fielddef.FieldDef{F: &deal.Transfer.Type},
			"TransferParams":        &fielddef.FieldDef{F: &deal.Transfer.Params},
			"TransferSize":          &fielddef.FieldDef{F: &deal.Transfer.Size},
			"ChainDealID":           &fielddef.FieldDef{F: &deal.ChainDealID},
			"PublishCID":            &fielddef.CidPtrFieldDef{F: &deal.PublishCID},
			"SectorID":              &fielddef.FieldDef{F: &deal.SectorID},
			"Offset":                &fielddef.FieldDef{F: &deal.Offset},
			"Length":                &fielddef.FieldDef{F: &deal.Length},
			"Checkpoint":            &fielddef.CkptFieldDef{F: &deal.Checkpoint},
			"CheckpointAt":          &fielddef.FieldDef{F: &deal.CheckpointAt},
			"Error":                 &fielddef.FieldDef{F: &deal.Err},
			"Retry":                 &fielddef.FieldDef{F: &deal.Retry},
			"FastRetrieval":         &fielddef.FieldDef{F: &deal.FastRetrieval},
			"AnnounceToIPNI":        &fielddef.FieldDef{F: &deal.AnnounceToIPNI},

			// Needed so the deal can be looked up by signed proposal cid
			"SignedProposalCID": &fielddef.SignedPropFieldDef{Prop: deal.ClientDealProposal},
		},
	}
}

func (d *dealAccessor) scan(row Scannable) error {
	return scan(dealFields, d.def, row)
}

func scan(fields []string, def map[string]fielddef.FieldDefinition, row Scannable) error {
	// For each field
	dest := []interface{}{}
	for _, name := range fields {
		// Get a pointer to the field that will receive the scanned value
		fieldDef := def[name]
		dest = append(dest, fieldDef.FieldPtr())
	}

	// Scan the row into each pointer
	err := row.Scan(dest...)
	if err != nil {
		return fmt.Errorf("scanning deal row: %w", err)
	}

	// For each field
	for name, fieldDef := range def {
		// Unmarshall the scanned value into deal object
		err := fieldDef.Unmarshall()
		if err != nil {
			return fmt.Errorf("unmarshalling db field %s: %s", name, err)
		}
	}
	return nil
}

func (d *dealAccessor) insert(ctx context.Context) error {
	return insert(ctx, "Deals", dealFields, dealFieldsStr, d.def, d.db)
}

func insert(ctx context.Context, table string, fields []string, fieldsStr string, def map[string]fielddef.FieldDefinition, db *sql.DB) error {
	// For each field
	values := []interface{}{}
	placeholders := make([]string, 0, len(values))
	for _, name := range fields {
		// Add a placeholder "?"
		fieldDef := def[name]
		placeholders = append(placeholders, "?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.Marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the INSERT
	qry := "INSERT INTO " + table + " (" + fieldsStr + ") "
	qry += "VALUES (" + strings.Join(placeholders, ",") + ")"
	_, err := db.ExecContext(ctx, qry, values...)
	return err
}

func (d *dealAccessor) update(ctx context.Context) error {
	return update(ctx, "Deals", dealFields, d.def, d.db, d.deal.DealUuid)
}

func update(ctx context.Context, table string, fields []string, def map[string]fielddef.FieldDefinition, db *sql.DB, dealUuid uuid.UUID) error {
	// For each field
	values := []interface{}{}
	setNames := make([]string, 0, len(values))
	for _, name := range fields {
		// Skip the ID field
		if name == "ID" {
			continue
		}

		// Add "fieldName = ?"
		fieldDef := def[name]
		setNames = append(setNames, name+" = ?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.Marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the UPDATE
	qry := "UPDATE " + table + " "
	qry += "SET " + strings.Join(setNames, ", ")

	qry += "WHERE ID = ?"
	values = append(values, dealUuid)

	_, err := db.ExecContext(ctx, qry, values...)
	return err
}

type DealsDB struct {
	db *sql.DB
}

func NewDealsDB(db *sql.DB) *DealsDB {
	return &DealsDB{db: db}
}

func (d *DealsDB) Insert(ctx context.Context, deal *types.ProviderDealState) error {
	return d.newDealDef(deal).insert(ctx)
}

func (d *DealsDB) Update(ctx context.Context, deal *types.ProviderDealState) error {
	return d.newDealDef(deal).update(ctx)
}

func (d *DealsDB) ByID(ctx context.Context, id uuid.UUID) (*types.ProviderDealState, error) {
	qry := "SELECT " + dealFieldsStr + " FROM Deals WHERE id=?"
	row := d.db.QueryRowContext(ctx, qry, id)
	return d.scanRow(row)
}

func (d *DealsDB) ByPublishCID(ctx context.Context, publishCid string) ([]*types.ProviderDealState, error) {
	qry := "SELECT " + dealFieldsStr + " FROM Deals WHERE PublishCID=?"
	rows, err := d.db.QueryContext(ctx, qry, publishCid)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var deals []*types.ProviderDealState
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

func (d *DealsDB) ByPieceCID(ctx context.Context, pieceCid cid.Cid) ([]*types.ProviderDealState, error) {
	return d.list(ctx, 0, 0, "PieceCID=?", pieceCid.String())
}

func (d *DealsDB) ByRootPayloadCID(ctx context.Context, payloadCid cid.Cid) ([]*types.ProviderDealState, error) {
	return d.list(ctx, 0, 0, "DealDataRoot=?", payloadCid.String())
}

func (d *DealsDB) BySignedProposalCID(ctx context.Context, proposalCid cid.Cid) (*types.ProviderDealState, error) {
	qry := "SELECT " + dealFieldsStr + " FROM Deals WHERE SignedProposalCID=?"
	row := d.db.QueryRowContext(ctx, qry, proposalCid.String())
	return d.scanRow(row)
}

func (d *DealsDB) BySectorID(ctx context.Context, sectorID abi.SectorID) ([]*types.ProviderDealState, error) {
	addr, err := address.NewIDAddress(uint64(sectorID.Miner))
	if err != nil {
		return nil, fmt.Errorf("creating address from ID %d: %w", sectorID.Miner, err)
	}

	return d.list(ctx, 0, 0, "ProviderAddress=? AND SectorID=?", addr.String(), sectorID.Number)
}

func (d *DealsDB) Count(ctx context.Context, query string, filter *FilterOptions) (int, error) {
	whereArgs := []interface{}{}
	where := "SELECT count(*) FROM Deals"
	if query != "" {
		searchWhere, searchArgs := withSearchQuery(searchFields, query, true)
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

func (d *DealsDB) ListActive(ctx context.Context) ([]*types.ProviderDealState, error) {
	return d.list(ctx, 0, 0, "Checkpoint != ?", dealcheckpoints.Complete.String())
}

func (d *DealsDB) ListCompleted(ctx context.Context) ([]*types.ProviderDealState, error) {
	return d.list(ctx, 0, 0, "Checkpoint = ?", dealcheckpoints.Complete.String())
}

func (d *DealsDB) List(ctx context.Context, query string, filter *FilterOptions, cursor *graphql.ID, offset int, limit int) ([]*types.ProviderDealState, error) {
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
		searchWhere, searchArgs := withSearchQuery(searchFields, query, true)
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

func withSearchFilter(filter FilterOptions) (string, []interface{}) {
	whereArgs := []interface{}{}
	statements := []string{}

	if filter.Checkpoint != nil {
		statements = append(statements, "Checkpoint = ?")
		whereArgs = append(whereArgs, *filter.Checkpoint)
	}

	if filter.IsOffline != nil {
		statements = append(statements, "IsOffline = ?")
		whereArgs = append(whereArgs, *filter.IsOffline)
	}

	if filter.TransferType != nil {
		statements = append(statements, "TransferType = ?")
		whereArgs = append(whereArgs, *filter.TransferType)
	}

	if filter.IsVerified != nil {
		statements = append(statements, "VerifiedDeal = ?")
		whereArgs = append(whereArgs, *filter.IsVerified)
	}

	if len(statements) == 0 {
		return "", whereArgs
	}

	where := "(" + strings.Join(statements, " AND ") + ")"
	return where, whereArgs
}

var searchFields = []string{"ID", "PieceCID", "ChainDealID", "ClientAddress", "ProviderAddress", "ClientPeerID", "DealDataRoot", "PublishCID", "SignedProposalCID"}

func withSearchQuery(fields []string, query string, searchLabel bool) (string, []interface{}) {
	query = strings.Trim(query, " \t\n")

	whereArgs := []interface{}{}
	where := "("
	for _, searchField := range fields {
		where += searchField + " = ? OR "
		whereArgs = append(whereArgs, query)
	}

	if searchLabel {
		// The label field is prefixed by the ' character
		// Note: In sqlite the concat operator is ||
		// Note: To escape a ' character it is prefixed by another '.
		// So when you put a ' in quotes, you have to write ''''
		where += "Label = ('''' || ?) OR "
		whereArgs = append(whereArgs, query)
	}

	where += " instr(Error, ?) > 0"
	whereArgs = append(whereArgs, query)
	where += ")"

	return where, whereArgs
}

func (d *DealsDB) list(ctx context.Context, offset int, limit int, whereClause string, whereArgs ...interface{}) ([]*types.ProviderDealState, error) {
	args := whereArgs
	qry := "SELECT " + dealFieldsStr + " FROM Deals"
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

	deals := make([]*types.ProviderDealState, 0, 16)
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

func (d *DealsDB) scanRow(row Scannable) (*types.ProviderDealState, error) {
	var deal types.ProviderDealState
	deal.ClientDealProposal = market.ClientDealProposal{
		Proposal: market.DealProposal{},
	}
	err := d.newDealDef(&deal).scan(row)
	return &deal, err
}
