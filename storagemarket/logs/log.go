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
	ctx       context.Context
	logger    *logging.ZapEventLogger
	logsDB    *db.LogsDB
	subsystem string
}

func NewDealLogger(ctx context.Context, logsDB *db.LogsDB) *DealLogger {
	return &DealLogger{
		ctx:    ctx,
		logger: baseLogger,
		logsDB: logsDB,
	}
}

func (d *DealLogger) Subsystem(name string) *DealLogger {
	return &DealLogger{
		ctx:       d.ctx,
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
	d.Errorw(dealId, errMsg, "err", err)
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
	if err := d.logsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}

func paramsWithDealID(dealId uuid.UUID, kvs ...interface{}) []interface{} {
	kvs = append([]interface{}{"id", dealId}, kvs...)
	return kvs
}
