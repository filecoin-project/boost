package rtvllog

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
	"time"

	datatransfer2 "github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

//go:embed create_retrieval_db.sql
var createRetrievalDBSQL string

func CreateTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, createRetrievalDBSQL); err != nil {
		return fmt.Errorf("failed to create retrieval DB tables: %w", err)
	}
	return nil
}

type RetrievalDealState struct {
	RowID                   uint64
	CreatedAt               time.Time
	UpdatedAt               time.Time
	LocalPeerID             peer.ID
	PeerID                  peer.ID
	DealID                  legacyretrievaltypes.DealID
	TransferID              datatransfer2.TransferID
	PayloadCID              cid.Cid
	PieceCID                *cid.Cid
	PaymentInterval         uint64
	PaymentIntervalIncrease uint64
	PricePerByte            abi.TokenAmount
	UnsealPrice             abi.TokenAmount
	Status                  string
	Message                 string
	TotalSent               uint64
	DTStatus                string
	DTMessage               string
}

type RetrievalLogDB struct {
	db *sql.DB
}

func NewRetrievalLogDB(db *sql.DB) *RetrievalLogDB {
	return &RetrievalLogDB{db}
}

func (d *RetrievalLogDB) Insert(ctx context.Context, l *RetrievalDealState) error {
	qry := "INSERT INTO RetrievalDealStates (" +
		"CreatedAt, " +
		"UpdatedAt, " +
		"LocalPeerID, " +
		"PeerID, " +
		"DealID, " +
		"TransferID, " +
		"PayloadCID, " +
		"PieceCID, " +
		"PaymentInterval, " +
		"PaymentIntervalIncrease, " +
		"PricePerByte, " +
		"UnsealPrice, " +
		"Status, " +
		"Message, " +
		"TotalSent, " +
		"DTStatus, " +
		"DTMessage" +
		") "
	qry += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '', '')"

	pieceCid := ""
	if l.PieceCID != nil {
		pieceCid = l.PieceCID.String()
	}

	now := time.Now()
	values := []interface{}{
		now,
		now,
		l.LocalPeerID.String(),
		l.PeerID.String(),
		l.DealID,
		l.TransferID,
		l.PayloadCID.String(),
		pieceCid,
		l.PaymentInterval,
		l.PaymentIntervalIncrease,
		l.PricePerByte.String(),
		l.UnsealPrice.String(),
		l.Status,
		l.Message,
	}
	_, err := d.db.ExecContext(ctx, qry, values...)
	return err
}

func (d *RetrievalLogDB) Get(ctx context.Context, peerID string, transferID uint64) (*RetrievalDealState, error) {
	rows, err := d.list(ctx, 0, 0, "PeerID = ? AND TransferID = ?", peerID, transferID)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no retrieval found with peer ID %s and transfer ID %d", peerID, transferID)
	}
	return &rows[0], nil
}

func (d *RetrievalLogDB) List(ctx context.Context, isIndexer *bool, cursor *uint64, offset int, limit int) ([]RetrievalDealState, error) {
	where := ""
	whereArgs := []interface{}{}
	if isIndexer != nil {
		if *isIndexer {
			where += "DealID = 0"
		} else {
			where += "DealID != 0"
		}
	}
	if cursor != nil {
		if where != "" {
			where += " AND "
		}
		where += "RowID <= ?"
		whereArgs = append(whereArgs, *cursor)
	}
	return d.list(ctx, offset, limit, where, whereArgs...)
}

func (d *RetrievalLogDB) ListLastUpdatedAndOpen(ctx context.Context, lastUpdated time.Time) ([]RetrievalDealState, error) {
	where := "UpdatedAt <= ?" +
		"AND Status != 'DealStatusCompleted'" +
		"AND Status != 'DealStatusCancelled'" +
		"AND Status != 'DealStatusErrored'" +
		"AND Status != 'DealStatusRejected'"

	return d.list(ctx, 0, 0, where, lastUpdated)
}

func (d *RetrievalLogDB) list(ctx context.Context, offset int, limit int, where string, whereArgs ...interface{}) ([]RetrievalDealState, error) {
	qry := "SELECT " +
		"RowID, " +
		"CreatedAt, " +
		"UpdatedAt, " +
		"LocalPeerID, " +
		"PeerID, " +
		"DealID, " +
		"TransferID, " +
		"PayloadCID, " +
		"PieceCID, " +
		"PaymentInterval, " +
		"PaymentIntervalIncrease, " +
		"PricePerByte, " +
		"UnsealPrice, " +
		"Status, " +
		"Message, " +
		"TotalSent, " +
		"DTStatus, " +
		"DTMessage " +
		"FROM RetrievalDealStates"

	if where != "" {
		qry += " WHERE " + where
	}
	qry += " ORDER BY RowID desc"

	args := append([]interface{}{}, whereArgs...)
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

	dealStates := make([]RetrievalDealState, 0, 16)
	for rows.Next() {
		var payloadCid sql.NullString
		var pieceCid sql.NullString
		var pricePerByte sql.NullString
		var unsealPrice sql.NullString
		var localPeerID sql.NullString
		var peerID sql.NullString

		var dealState RetrievalDealState
		err := rows.Scan(
			&dealState.RowID,
			&dealState.CreatedAt,
			&dealState.UpdatedAt,
			&localPeerID,
			&peerID,
			&dealState.DealID,
			&dealState.TransferID,
			&payloadCid,
			&pieceCid,
			&dealState.PaymentInterval,
			&dealState.PaymentIntervalIncrease,
			&pricePerByte,
			&unsealPrice,
			&dealState.Status,
			&dealState.Message,
			&dealState.TotalSent,
			&dealState.DTStatus,
			&dealState.DTMessage,
		)
		if err != nil {
			return nil, err
		}

		if localPeerID.String != "" {
			dealState.LocalPeerID, err = peer.Decode(localPeerID.String)
			if err != nil {
				return nil, fmt.Errorf("parsing local peer ID '%s': %w", localPeerID.String, err)
			}
		}
		if peerID.String != "" {
			dealState.PeerID, err = peer.Decode(peerID.String)
			if err != nil {
				return nil, fmt.Errorf("parsing peer ID '%s': %w", peerID.String, err)
			}
		}

		dealState.PayloadCID, err = cid.Parse(payloadCid.String)
		if err != nil {
			dealState.PayloadCID = cid.Undef
		}

		if pieceCid.Valid && pieceCid.String != "" {
			c, err := cid.Parse(pieceCid.String)
			if err != nil {
				return nil, fmt.Errorf("parsing piece cid '%s': %w", pieceCid.String, err)
			}
			dealState.PieceCID = &c
		}

		dealState.PricePerByte = big.NewInt(0)
		dealState.PricePerByte.SetString(pricePerByte.String, 0)

		dealState.UnsealPrice = big.NewInt(0)
		dealState.UnsealPrice.SetString(unsealPrice.String, 0)

		dealStates = append(dealStates, dealState)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return dealStates, nil
}

func (d *RetrievalLogDB) Count(ctx context.Context, isIndexer *bool) (int, error) {
	var count int
	qry := "SELECT count(*) FROM RetrievalDealStates"
	if isIndexer != nil {
		if *isIndexer {
			qry += " WHERE DealID = 0"
		} else {
			qry += " WHERE DealID != 0"
		}
	}
	row := d.db.QueryRowContext(ctx, qry)
	err := row.Scan(&count)
	return count, err
}

func (d *RetrievalLogDB) Update(ctx context.Context, state legacyretrievaltypes.ProviderDealState) error {
	fields := map[string]interface{}{
		"Status":    state.Status.String(),
		"TotalSent": state.TotalSent,
		"Message":   state.Message,
		"UpdatedAt": time.Now(),
	}
	where := "PeerID = ? AND DealID = ?"
	args := []interface{}{state.Receiver.String(), state.ID}
	if state.ChannelID != nil {
		where += " AND TransferID = ?"
		args = append(args, state.ChannelID.ID)
		fields["TransferID"] = state.ChannelID.ID
		fields["LocalPeerID"] = state.ChannelID.Responder.String()
	}

	return d.update(ctx, fields, where, args...)
}

func (d *RetrievalLogDB) UpdateDataTransferState(ctx context.Context, event datatransfer2.Event, state datatransfer2.ChannelState) error {
	peerID := state.OtherPeer().String()
	transferID := state.TransferID()
	if err := d.insertDTEvent(ctx, peerID, transferID, event); err != nil {
		return err
	}

	fields := map[string]interface{}{
		"DTStatus":  datatransfer2.Statuses[state.Status()],
		"DTMessage": state.Message(),
		"UpdatedAt": time.Now(),
	}
	return d.update(ctx, fields, "PeerID = ? AND TransferID = ?", peerID, transferID)
}

func (d *RetrievalLogDB) update(ctx context.Context, fields map[string]interface{}, where string, whereArgs ...interface{}) error {
	setNames := []string{}
	values := []interface{}{}
	for name, val := range fields {
		setNames = append(setNames, name+" = ?")
		values = append(values, val)
	}
	set := strings.Join(setNames, ", ")

	qry := "UPDATE RetrievalDealStates SET " + set + " WHERE " + where
	_, err := d.db.ExecContext(ctx, qry, append(values, whereArgs...)...)
	return err
}

func (d *RetrievalLogDB) insertDTEvent(ctx context.Context, peerID string, transferID datatransfer2.TransferID, event datatransfer2.Event) error {
	qry := "INSERT INTO RetrievalDataTransferEvents (PeerID, TransferID, CreatedAt, Name, Message) " +
		"VALUES (?, ?, ?, ?, ?)"
	_, err := d.db.ExecContext(ctx, qry, peerID, transferID, event.Timestamp, datatransfer2.Events[event.Code], event.Message)
	return err
}

type DTEvent struct {
	CreatedAt time.Time
	Name      string
	Message   string
}

func (d *RetrievalLogDB) ListDTEvents(ctx context.Context, peerID string, transferID datatransfer2.TransferID) ([]DTEvent, error) {
	qry := "SELECT CreatedAt, Name, Message " +
		"FROM RetrievalDataTransferEvents " +
		"WHERE PeerID = ? AND TransferID = ? " +
		"ORDER BY CreatedAt desc"

	rows, err := d.db.QueryContext(ctx, qry, peerID, transferID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	dtEvents := make([]DTEvent, 0, 16)
	for rows.Next() {
		var dtEvent DTEvent
		err := rows.Scan(
			&dtEvent.CreatedAt,
			&dtEvent.Name,
			&dtEvent.Message,
		)
		if err != nil {
			return nil, err
		}

		dtEvents = append(dtEvents, dtEvent)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return dtEvents, nil
}

func (d *RetrievalLogDB) InsertMarketsEvent(ctx context.Context, event legacyretrievaltypes.ProviderEvent, state legacyretrievaltypes.ProviderDealState) error {
	// Ignore block sent events as we are recording the equivalent event for
	// data-transfer, and it's a high-frequency event
	if event == legacyretrievaltypes.ProviderEventBlockSent {
		return nil
	}

	qry := "INSERT INTO RetrievalMarketEvents (PeerID, DealID, CreatedAt, Name, Status, Message) " +
		"VALUES (?, ?, ?, ?, ?, ?)"
	_, err := d.db.ExecContext(ctx, qry,
		state.Receiver.String(),
		state.ID,
		time.Now(),
		legacyretrievaltypes.ProviderEvents[event],
		state.Status.String(),
		state.Message)
	return err
}

type MarketEvent struct {
	CreatedAt time.Time
	Name      string
	Status    string
	Message   string
}

func (d *RetrievalLogDB) ListMarketEvents(ctx context.Context, peerID string, dealID legacyretrievaltypes.DealID) ([]MarketEvent, error) {
	qry := "SELECT CreatedAt, Name, Status, Message " +
		"FROM RetrievalMarketEvents " +
		"WHERE PeerID = ? AND DealID = ? " +
		"ORDER BY CreatedAt desc"

	rows, err := d.db.QueryContext(ctx, qry, peerID, dealID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	evts := make([]MarketEvent, 0, 16)
	for rows.Next() {
		var evt MarketEvent
		err := rows.Scan(
			&evt.CreatedAt,
			&evt.Name,
			&evt.Status,
			&evt.Message,
		)
		if err != nil {
			return nil, err
		}

		evts = append(evts, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return evts, nil
}

func (d *RetrievalLogDB) DeleteOlderThan(ctx context.Context, at time.Time) (int64, error) {
	_, err := d.db.ExecContext(ctx, "DELETE FROM RetrievalMarketEvents WHERE CreatedAt < ?", at)
	if err != nil {
		return 0, err
	}

	_, err = d.db.ExecContext(ctx, "DELETE FROM RetrievalDataTransferEvents WHERE CreatedAt < ?", at)
	if err != nil {
		return 0, err
	}

	res, err := d.db.ExecContext(ctx, "DELETE FROM RetrievalDealStates WHERE CreatedAt < ?", at)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}
