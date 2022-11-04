package logs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/filecoin-project/boost/db"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

var baseLogger = logging.Logger("boost-storage-deal")

type DealLogger struct {
	logger    *logging.ZapEventLogger
	logsDB    *db.LogsDB
	subsystem string
}

func NewDealLogger(logsDB *db.LogsDB) *DealLogger {
	return &DealLogger{
		logger: baseLogger,
		logsDB: logsDB,
	}
}

func (d *DealLogger) Subsystem(name string) *DealLogger {
	return &DealLogger{
		logger:    logging.Logger(d.subsystem + name),
		logsDB:    d.logsDB,
		subsystem: name,
	}
}

func (d *DealLogger) Infow(dealId uuid.UUID, msg string, kvs ...interface{}) {
	kvs = paramsWithDealID(dealId, kvs...)

	d.logger.Infow(msg, kvs...)
	d.updateLogDB(dealId, msg, "INFO", kvs...)
}

func (d *DealLogger) Warnw(dealId uuid.UUID, msg string, kvs ...interface{}) {
	kvs = paramsWithDealID(dealId, kvs...)
	d.logger.Warnw(msg, kvs...)
	d.updateLogDB(dealId, msg, "WARN", kvs...)
}

func (d *DealLogger) Errorw(dealId uuid.UUID, errMsg string, kvs ...interface{}) {
	kvs = paramsWithDealID(dealId, kvs...)

	d.logger.Errorw(errMsg, kvs...)
	d.updateLogDB(dealId, errMsg, "ERROR", kvs...)
}

func (d *DealLogger) LogError(dealId uuid.UUID, errMsg string, err error) {
	d.Errorw(dealId, errMsg, "err", err.Error())
}

func (d *DealLogger) updateLogDB(dealId uuid.UUID, msg string, level string, kvs ...interface{}) {
	jsn, err := json.Marshal(kvs)
	if err != nil {
		d.logger.Warnw("failed to marshal log params to json", "err", err, "id", dealId)
	}

	l := &db.DealLog{
		DealUUID:  dealId,
		CreatedAt: time.Now(),
		LogLevel:  level,
		LogMsg:    msg,
		LogParams: string(jsn),
		Subsystem: d.subsystem,
	}
	// we don't want context cancellations to mess up our logging, so pass a background context
	if err := d.logsDB.InsertLog(context.Background(), l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}

func paramsWithDealID(dealId uuid.UUID, kvs ...interface{}) []interface{} {
	kvs = append([]interface{}{"id", dealId}, kvs...)
	return kvs
}

func (d *DealLogger) LogCleanup(ctx context.Context, DealLogDurationDays int) {

	// Create a ticker with an hour tick
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.logger.Infof("Cleaning logs older than %d days from logsDB ", DealLogDurationDays)
			err := d.logsDB.CleanupLogs(ctx, DealLogDurationDays)
			if err != nil {
				d.logger.Errorf("Failed to cleanup old logs from logsDB: %s", err)
			}

		case <-ctx.Done():
			return
		}
	}
}
