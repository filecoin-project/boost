package db

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

type Scannable interface {
	Scan(dest ...interface{}) error
}

type DealLog struct {
	DealUuid  uuid.UUID
	CreatedAt time.Time
	Text      string
}

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
			"ID":                    &fieldDef{f: &deal.DealUuid},
			"CreatedAt":             &fieldDef{f: &deal.CreatedAt},
			"DealProposalSignature": &sigFieldDef{f: &deal.ClientDealProposal.ClientSignature},
			"PieceCID":              &cidFieldDef{f: &deal.ClientDealProposal.Proposal.PieceCID},
			"PieceSize":             &fieldDef{f: &deal.ClientDealProposal.Proposal.PieceSize},
			"VerifiedDeal":          &fieldDef{f: &deal.ClientDealProposal.Proposal.VerifiedDeal},
			"ClientAddress":         &addrFieldDef{f: &deal.ClientDealProposal.Proposal.Client},
			"ProviderAddress":       &addrFieldDef{f: &deal.ClientDealProposal.Proposal.Provider},
			"Label":                 &fieldDef{f: &deal.ClientDealProposal.Proposal.Label},
			"StartEpoch":            &fieldDef{f: &deal.ClientDealProposal.Proposal.StartEpoch},
			"EndEpoch":              &fieldDef{f: &deal.ClientDealProposal.Proposal.EndEpoch},
			"StoragePricePerEpoch":  &bigIntFieldDef{f: &deal.ClientDealProposal.Proposal.StoragePricePerEpoch},
			"ProviderCollateral":    &bigIntFieldDef{f: &deal.ClientDealProposal.Proposal.ProviderCollateral},
			"ClientCollateral":      &bigIntFieldDef{f: &deal.ClientDealProposal.Proposal.ClientCollateral},
			"SelfPeerID":            &fieldDef{f: &deal.SelfPeerID},
			"ClientPeerID":          &fieldDef{f: &deal.ClientPeerID},
			"DealDataRoot":          &cidFieldDef{f: &deal.DealDataRoot},
			"InboundFilePath":       &fieldDef{f: &deal.InboundFilePath},
			"TransferType":          &fieldDef{f: &deal.TransferType},
			"TransferParams":        &fieldDef{f: &deal.TransferParams},
			"ChainDealID":           &fieldDef{f: &deal.ChainDealID},
			"PublishCID":            &cidFieldDef{f: deal.PublishCID},
			"SectorID":              &fieldDef{f: &deal.SectorID},
			"Offset":                &fieldDef{f: &deal.Offset},
			"Length":                &fieldDef{f: &deal.Length},
			"Checkpoint":            &ckptFieldDef{f: &deal.Checkpoint},
			"Error":                 &fieldDef{f: &deal.Err},
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
	for _, fieldDef := range d.def {
		// Unmarshall the scanned value into deal object
		err := fieldDef.unmarshall()
		if err != nil {
			return xerrors.Errorf("unmarshalling db field: %s", err)
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

// OnUpdateFn is called when the deal is updated in the database
type OnUpdateFn func(deal *types.ProviderDealState)

type DealsDB struct {
	db         *sql.DB
	onUpdateFn OnUpdateFn
}

func SqlDB(dbPath string) (*sql.DB, error) {
	return sql.Open("sqlite3", "file:"+dbPath)
}

func Open(dbPath string) (*DealsDB, error) {
	db, err := SqlDB(dbPath)
	if err != nil {
		return nil, err
	}
	return NewDealsDB(db), nil
}

func NewDealsDB(db *sql.DB) *DealsDB {
	return &DealsDB{db: db}
}

func (d *DealsDB) Insert(ctx context.Context, deal *types.ProviderDealState) error {
	return d.newDealDef(deal).insert(ctx)
}

func (d *DealsDB) Update(ctx context.Context, deal *types.ProviderDealState) error {
	err := d.newDealDef(deal).update(ctx)
	if err == nil && d.onUpdateFn != nil {
		d.onUpdateFn(deal)
	}
	return err
}

func (d *DealsDB) OnUpdate(f OnUpdateFn) {
	d.onUpdateFn = f
}

func (d *DealsDB) ByID(ctx context.Context, id uuid.UUID) (*types.ProviderDealState, error) {
	qry := "SELECT " + dealFieldsStr + " FROM Deals WHERE id=?"
	row := d.db.QueryRowContext(ctx, qry, id)
	return d.scanRow(row)
}

func (d *DealsDB) ListActive(ctx context.Context) ([]*types.ProviderDealState, error) {
	return d.list(ctx, "Checkpoint != ?", dealcheckpoints.Complete.String())
}

func (d *DealsDB) List(ctx context.Context) ([]*types.ProviderDealState, error) {
	return d.list(ctx, "")
}

func (d *DealsDB) list(ctx context.Context, whereClause string, whereArgs ...interface{}) ([]*types.ProviderDealState, error) {
	qry := "SELECT " + dealFieldsStr + " FROM Deals"
	if whereClause != "" {
		qry += " WHERE " + whereClause
	}
	qry += " ORDER BY CreatedAt DESC"

	rows, err := d.db.QueryContext(ctx, qry, whereArgs...)
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

func (d *DealsDB) InsertLog(ctx context.Context, l *DealLog) error {
	qry := "INSERT INTO DealLogs (DealID, CreatedAt, LogText) "
	qry += "VALUES (?, ?, ?)"
	values := []interface{}{l.DealUuid, l.CreatedAt, l.Text}
	_, err := d.db.ExecContext(ctx, qry, values...)
	return err
}

func (d *DealsDB) Logs(ctx context.Context, dealID uuid.UUID) ([]DealLog, error) {
	qry := "SELECT DealID, CreatedAt, LogText FROM DealLogs WHERE DealID=?"
	rows, err := d.db.QueryContext(ctx, qry, dealID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dealLogs := make([]DealLog, 0, 16)
	for rows.Next() {
		var dealLog DealLog
		err := rows.Scan(
			&dealLog.DealUuid,
			&dealLog.CreatedAt,
			&dealLog.Text)

		if err != nil {
			return nil, err
		}
		dealLogs = append(dealLogs, dealLog)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return dealLogs, nil
}

func (d *DealsDB) Init(ctx context.Context) error {
	return createTables(ctx, d.db)
}

func createTables(ctx context.Context, db *sql.DB) error {
	_, filename, _, _ := runtime.Caller(1)
	createPath := path.Join(path.Dir(filename), "/create.sql")
	createScript, err := ioutil.ReadFile(createPath)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, string(createScript))
	return err
}

func CreateTmpDB() (string, error) {
	tmpFile, err := os.CreateTemp("", "test.db")
	if err != nil {
		return "", err
	}
	tmpFilePath := tmpFile.Name()

	db, err := sql.Open("sqlite3", "file:"+tmpFilePath)
	if err != nil {
		return "", err
	}

	defer db.Close()

	ctx := context.Background()
	_, err = LoadFixtures(ctx, db)

	return tmpFilePath, err
}
