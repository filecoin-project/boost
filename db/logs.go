package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
)

type DealLog struct {
	DealUUID  uuid.UUID
	CreatedAt time.Time
	LogLevel  string
	LogMsg    string
	LogParams string
	Subsystem string
}

type LogsDB struct {
	db *sql.DB
}

func NewLogsDB(db *sql.DB) *LogsDB {
	return &LogsDB{db}
}

func (d *LogsDB) InsertLog(ctx context.Context, l *DealLog) error {
	qry := "INSERT INTO DealLogs (DealUUID, CreatedAt, LogLevel, LogMsg, LogParams, Subsystem) "
	qry += "VALUES (?, ?, ?, ?, ?, ?)"
	values := []interface{}{l.DealUUID.String(), l.CreatedAt, l.LogLevel, l.LogMsg, l.LogParams, l.Subsystem}
	_, err := d.db.ExecContext(ctx, qry, values...)
	return err
}

func (d *LogsDB) Logs(ctx context.Context, dealID uuid.UUID) ([]DealLog, error) {
	qry := "SELECT DealUUID, CreatedAt, LogLevel, LogMsg, LogParams, Subsystem FROM DealLogs WHERE DealUUID=?"
	rows, err := d.db.QueryContext(ctx, qry, dealID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	dealLogs := make([]DealLog, 0, 16)
	for rows.Next() {
		var dealLog DealLog
		err := rows.Scan(
			&dealLog.DealUUID,
			&dealLog.CreatedAt,
			&dealLog.LogLevel,
			&dealLog.LogMsg,
			&dealLog.LogParams,
			&dealLog.Subsystem)

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

func (d *LogsDB) CleanupLogs(ctx context.Context, daysOld int) error {

	t := time.Now()
	td := t.AddDate(0, 0, -1*daysOld)

	qry := "DELETE from DealLogs WHERE DealUUID IN (SELECT DISTINCT DealUUID FROM DealLogs WHERE CreatedAt < ?)"

	_, err := d.db.ExecContext(ctx, qry, td)
	return err
}
