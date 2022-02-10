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
	ctx     context.Context
	logger  *logging.ZapEventLogger
	dealsDB *db.DealsDB
}

func NewDealLogger(ctx context.Context, dealsDB *db.DealsDB) *DealLogger {
	return &DealLogger{
		ctx:     ctx,
		logger:  logging.Logger("boost-storage-deal"),
		dealsDB: dealsDB,
	}
}

func (d *DealLogger) Infow(dealId uuid.UUID, msg string, kvs ...interface{}) {
	d.logger.Infow(msg, "id", dealId, kvs)

	l := &db.DealLog{
		DealUUID:  dealId,
		Text:      fmt.Sprintf("INFO message: %s, args: %+v", msg, kvs),
		CreatedAt: time.Now(),
	}
	if err := d.dealsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}

func (d *DealLogger) Warnw(dealId uuid.UUID, msg string, kvs ...interface{}) {
	d.logger.Warnw(msg, "id", dealId, kvs)

	l := &db.DealLog{
		DealUUID:  dealId,
		Text:      fmt.Sprintf("WARN message: %s, args: %+v", msg, kvs),
		CreatedAt: time.Now(),
	}
	if err := d.dealsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}

func (d *DealLogger) Errorw(dealId uuid.UUID, errMsg string, err error) {
	d.logger.Errorw(errMsg, "id", dealId, "err", err)

	l := &db.DealLog{
		DealUUID:  dealId,
		Text:      fmt.Sprintf("ERROR message: %s, err: %s", errMsg, err),
		CreatedAt: time.Now(),
	}
	if err := d.dealsDB.InsertLog(d.ctx, l); err != nil {
		d.logger.Warnw("failed to persist deal log", "id", dealId, "err", err)
	}
}
