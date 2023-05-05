package types

import (
	"errors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
	cborgen "github.com/whyrusleeping/cbor-gen"
)

//go:generate go run -tags cbg ../tools

var (
	_ datatransfer.RequestValidator = (*dagsyncValidator)(nil)

	_ datatransfer.Registerable = (*LegsVoucher)(nil)
	_ cborgen.CBORMarshaler     = (*LegsVoucher)(nil)
	_ cborgen.CBORUnmarshaler   = (*LegsVoucher)(nil)

	_ datatransfer.Registerable = (*LegsVoucherResult)(nil)
	_ cborgen.CBORMarshaler     = (*LegsVoucherResult)(nil)
	_ cborgen.CBORUnmarshaler   = (*LegsVoucherResult)(nil)
)

// A Voucher is used to communicate a new DAG head
type LegsVoucher struct {
	Head *cid.Cid
}

// Type provides an identifier for the voucher to go-data-transfer
func (v *LegsVoucher) Type() datatransfer.TypeIdentifier {
	return "LegsVoucher"
}

// A VoucherResult responds to a voucher
type LegsVoucherResult struct {
	Code uint64
}

// Type provides an identifier for the voucher result to go-data-transfer
func (v *LegsVoucherResult) Type() datatransfer.TypeIdentifier {
	return "LegsVoucherResult"
}

type dagsyncValidator struct {
	//ctx context.Context
	//ValidationsReceived chan receivedValidation
	allowPeer func(peer.ID) bool
}

func (vl *dagsyncValidator) ValidatePush(
	_ bool,
	_ datatransfer.ChannelID,
	_ peer.ID,
	_ datatransfer.Voucher,
	_ cid.Cid,
	_ ipld.Node) (datatransfer.VoucherResult, error) {

	// This is a pull-only DT voucher.
	return nil, errors.New("invalid")
}

func (vl *dagsyncValidator) ValidatePull(
	_ bool,
	_ datatransfer.ChannelID,
	peerID peer.ID,
	voucher datatransfer.Voucher,
	_ cid.Cid,
	_ ipld.Node) (datatransfer.VoucherResult, error) {

	v := voucher.(*LegsVoucher)
	if v.Head == nil {
		return nil, errors.New("invalid")
	}

	if vl.allowPeer != nil && !vl.allowPeer(peerID) {
		return nil, errors.New("peer not allowed")
	}

	return &LegsVoucherResult{}, nil
}
