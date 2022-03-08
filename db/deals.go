package db

import (
	"context"
	"database/sql"
	"strings"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
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
	def  map[string]fieldDefinition
}

func (d *DealsDB) newDealDef(deal *types.ProviderDealState) *dealAccessor {
	return newDealAccessor(d.db, deal)
}

func newDealAccessor(db *sql.DB, deal *types.ProviderDealState) *dealAccessor {
	return &dealAccessor{
		db:   db,
		deal: deal,
		def: map[string]fieldDefinition{
			"ID":                     &fieldDef{f: &deal.DealUuid},
			"CreatedAt":              &fieldDef{f: &deal.CreatedAt},
			"DealProposalSignature":  &sigFieldDef{f: &deal.ClientDealProposal.ClientSignature},
			"PieceCID":               &cidFieldDef{f: &deal.ClientDealProposal.Proposal.PieceCID},
			"PieceSize":              &fieldDef{f: &deal.ClientDealProposal.Proposal.PieceSize},
			"VerifiedDeal":           &fieldDef{f: &deal.ClientDealProposal.Proposal.VerifiedDeal},
			"ClientAddress":          &addrFieldDef{f: &deal.ClientDealProposal.Proposal.Client},
			"ProviderAddress":        &addrFieldDef{f: &deal.ClientDealProposal.Proposal.Provider},
			"Label":                  &fieldDef{f: &deal.ClientDealProposal.Proposal.Label},
			"StartEpoch":             &fieldDef{f: &deal.ClientDealProposal.Proposal.StartEpoch},
			"EndEpoch":               &fieldDef{f: &deal.ClientDealProposal.Proposal.EndEpoch},
			"StoragePricePerEpoch":   &bigIntFieldDef{f: &deal.ClientDealProposal.Proposal.StoragePricePerEpoch},
			"ProviderCollateral":     &bigIntFieldDef{f: &deal.ClientDealProposal.Proposal.ProviderCollateral},
			"ClientCollateral":       &bigIntFieldDef{f: &deal.ClientDealProposal.Proposal.ClientCollateral},
			"ClientPeerID":           &fieldDef{f: &deal.ClientPeerID},
			"DealDataRoot":           &cidFieldDef{f: &deal.DealDataRoot},
			"InboundFilePath":        &fieldDef{f: &deal.InboundFilePath},
			"TransferType":           &fieldDef{f: &deal.Transfer.Type},
			"TransferParams":         &fieldDef{f: &deal.Transfer.Params},
			"TransferSize":           &fieldDef{f: &deal.Transfer.Size},
			"ChainDealID":            &fieldDef{f: &deal.ChainDealID},
			"PublishCID":             &cidPtrFieldDef{f: &deal.PublishCID},
			"IndexerAnnouncementCID": &cidPtrFieldDef{f: &deal.IndexerAnnouncementCID},
			"SectorID":               &fieldDef{f: &deal.SectorID},
			"Offset":                 &fieldDef{f: &deal.Offset},
			"Length":                 &fieldDef{f: &deal.Length},
			"Checkpoint":             &ckptFieldDef{f: &deal.Checkpoint},
			"Error":                  &fieldDef{f: &deal.Err},
			// Needed so the deal can be looked up by signed proposal cid
			"SignedProposalCID": &signedPropFieldDef{prop: deal.ClientDealProposal},
		},
	}
}

func (d *dealAccessor) scan(row Scannable) error {
	// For each field
	dest := []interface{}{}
	for _, name := range dealFields {
		// Get a pointer to the field that will receive the scanned value
		fieldDef := d.def[name]
		dest = append(dest, fieldDef.fieldPtr())
	}

	// Scan the row into each pointer
	err := row.Scan(dest...)
	if err != nil {
		return xerrors.Errorf("scanning deal row: %w", err)
	}

	// For each field
	for name, fieldDef := range d.def {
		// Unmarshall the scanned value into deal object
		err := fieldDef.unmarshall()
		if err != nil {
			return xerrors.Errorf("unmarshalling db field %s: %s", name, err)
		}
	}
	return nil
}

func (d *dealAccessor) insert(ctx context.Context) error {
	// For each field
	values := []interface{}{}
	placeholders := make([]string, 0, len(values))
	for _, name := range dealFields {
		// Add a placeholder "?"
		fieldDef := d.def[name]
		placeholders = append(placeholders, "?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the INSERT
	qry := "INSERT INTO Deals (" + dealFieldsStr + ") "
	qry += "VALUES (" + strings.Join(placeholders, ",") + ")"
	_, err := d.db.ExecContext(ctx, qry, values...)
	return err
}

func (d *dealAccessor) update(ctx context.Context) error {
	// For each field
	values := []interface{}{}
	setNames := make([]string, 0, len(values))
	for _, name := range dealFields {
		// Skip the ID field
		if name == "ID" {
			continue
		}

		// Add "fieldName = ?"
		fieldDef := d.def[name]
		setNames = append(setNames, name+" = ?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the UPDATE
	qry := "UPDATE Deals "
	qry += "SET " + strings.Join(setNames, ", ")

	qry += "WHERE ID = ?"
	values = append(values, d.deal.DealUuid)

	_, err := d.db.ExecContext(ctx, qry, values...)
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

func (d *DealsDB) ByPublishCID(ctx context.Context, publishCid string) (*types.ProviderDealState, error) {
	qry := "SELECT " + dealFieldsStr + " FROM Deals WHERE PublishCID=?"
	row := d.db.QueryRowContext(ctx, qry, publishCid)
	return d.scanRow(row)
}

func (d *DealsDB) BySignedProposalCID(ctx context.Context, proposalCid string) (*types.ProviderDealState, error) {
	qry := "SELECT " + dealFieldsStr + " FROM Deals WHERE SignedProposalCID=?"
	row := d.db.QueryRowContext(ctx, qry, proposalCid)
	return d.scanRow(row)
}

func (d *DealsDB) Count(ctx context.Context) (int, error) {
	var count int
	row := d.db.QueryRowContext(ctx, "SELECT count(*) FROM Deals")
	err := row.Scan(&count)
	return count, err
}

func (d *DealsDB) ListActive(ctx context.Context) ([]*types.ProviderDealState, error) {
	return d.list(ctx, 0, "Checkpoint != ?", dealcheckpoints.Complete.String())
}

func (d *DealsDB) List(ctx context.Context, first *graphql.ID, limit int) ([]*types.ProviderDealState, error) {
	where := ""
	whereArgs := []interface{}{}
	if first != nil {
		where += "CreatedAt <= (SELECT CreatedAt FROM Deals WHERE ID = ?)"
		whereArgs = append(whereArgs, *first)
	}
	return d.list(ctx, limit, where, whereArgs...)
}

func (d *DealsDB) list(ctx context.Context, limit int, whereClause string, whereArgs ...interface{}) ([]*types.ProviderDealState, error) {
	args := whereArgs
	qry := "SELECT " + dealFieldsStr + " FROM Deals"
	if whereClause != "" {
		qry += " WHERE " + whereClause
	}
	qry += " ORDER BY CreatedAt DESC"
	if limit > 0 {
		qry += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := d.db.QueryContext(ctx, qry, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
