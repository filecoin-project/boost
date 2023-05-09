package types

import (
	datatransfer "github.com/filecoin-project/go-data-transfer"
	datatransfer2 "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/ipni/storetheindex/dagsync/dtsync"
)

type LegsVoucherDTv1 struct {
	dtsync.Voucher
}

func (l *LegsVoucherDTv1) Type() datatransfer.TypeIdentifier {
	return datatransfer.TypeIdentifier(dtsync.LegsVoucherType)
}

type LegsVoucherResultDtv1 struct {
	VoucherType datatransfer2.TypeIdentifier
	dtsync.VoucherResult
}

func (d *LegsVoucherResultDtv1) Type() datatransfer.TypeIdentifier {
	return datatransfer.TypeIdentifier(d.VoucherType)
}
