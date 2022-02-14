package logs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/db"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

type DealLogger struct {
	ctx    context.Context
	logger *logging.ZapEventLogger
	logsDB *db.LogsDB
	prefix string
}

func NewDealLogger(ctx context.Context, logsDB *db.LogsDB) *DealLogger {
	prefix := "boost-storage-deal"
	return &DealLogger{
		ctx:    ctx,
		logger: logging.Logger(prefix),
		logsDB: logsDB,
		prefix: prefix,
	}
}

func (d *DealLogger) Subsystem(name string) *DealLogger {
	prefix := d.prefix + "/" + name
	return &DealLogger{
		ctx:    d.ctx,
		logger: logging.Logger(prefix),
		logsDB: d.logsDB,
		prefix: prefix,
	}
}

func (d *DealLogger) Infow(dealId uuid.UUID, msg string, kvs ...interface{}) {
	d.logger.Infow(msg, "id", dealId, kvs)
	d.updateLogDB(dealId, msg, "INFO", kvs)
}

func (d *DealLogger) Warnw(dealId uuid.UUID, msg string, kvs ...interface{}) {
	d.logger.Warnw(msg, "id", dealId, kvs)
	d.updateLogDB(dealId, msg, "WARN", kvs)
}

func (d *DealLogger) Errorw(dealId uuid.UUID, errMsg string, kvs ...interface{}) {
	d.logger.Errorw(errMsg, "id", dealId, kvs)
	d.updateLogDB(dealId, errMsg, "ERROR", kvs)
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
		LogMsg:    d.prefix + ":" + msg,
		LogParams: string(jsn),
	}
	if err := d.logsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}

func (d *DealLogger) LogError(dealId uuid.UUID, errMsg string, err error) {
	d.logger.Errorw(errMsg, "id", dealId, "err", err)

	l := &db.DealLog{
		DealUUID:  dealId,
		CreatedAt: time.Now(),
		LogLevel:  "ERROR",
		LogMsg:    d.prefix + ":" + fmt.Sprintf("msg: %s, err: %s", errMsg, err),
	}
	if err := d.logsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}
