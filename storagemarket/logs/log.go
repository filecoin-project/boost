package logs

import (
	"context"
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
}

func NewDealLogger(ctx context.Context, logsDB *db.LogsDB) *DealLogger {
	return &DealLogger{
		ctx:    ctx,
		logger: logging.Logger("boost-storage-deal"),
		logsDB: logsDB,
	}
}

func (d *DealLogger) Infow(dealId uuid.UUID, msg string, kvs ...interface{}) {
	d.logger.Infow(msg, "id", dealId, kvs)
	l := &db.DealLog{
		DealUUID:  dealId,
		CreatedAt: time.Now(),
		LogLevel:  "INFO",
		LogMsg:    msg,
		LogParams: fmt.Sprintf("%+v", kvs),
	}

	if err := d.logsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}

func (d *DealLogger) Warnw(dealId uuid.UUID, msg string, kvs ...interface{}) {
	d.logger.Warnw(msg, "id", dealId, kvs)

	l := &db.DealLog{
		DealUUID:  dealId,
		CreatedAt: time.Now(),
		LogLevel:  "WARN",
		LogMsg:    msg,
		LogParams: fmt.Sprintf("%+v", kvs),
	}
	if err := d.logsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}

func (d *DealLogger) Errorw(dealId uuid.UUID, errMsg string, err error) {
	d.logger.Errorw(errMsg, "id", dealId, "err", err)

	l := &db.DealLog{
		DealUUID:  dealId,
		CreatedAt: time.Now(),
		LogLevel:  "ERROR",
		LogMsg:    fmt.Sprintf("msg: %s, err: %s", errMsg, err),
	}
	if err := d.logsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}
