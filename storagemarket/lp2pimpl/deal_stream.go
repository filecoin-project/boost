package lp2pimpl

import (
	"bufio"

	"github.com/filecoin-project/boost/storagemarket/types"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/mux"
	"github.com/libp2p/go-libp2p-core/peer"

	cborutil "github.com/filecoin-project/go-cbor-util"
)

// DealStream reads and writes deal proposals to a libp2p stream
type DealStream struct {
	p        peer.ID
	host     host.Host
	rw       mux.MuxedStream
	buffered *bufio.Reader
}

func (d *DealStream) ReadDealProposal() (types.DealParams, error) {
	var ds types.DealParams
	if err := ds.UnmarshalCBOR(d.buffered); err != nil {
		return types.DealParams{}, err
	}
	return ds, nil
}

func (d *DealStream) WriteDealProposal(dp types.DealParams) error {
	return cborutil.WriteCborRPC(d.rw, &dp)
}

func (d *DealStream) ReadDealResponse() (types.DealResponse, error) {
	var dr types.DealResponse
	if err := dr.UnmarshalCBOR(d.buffered); err != nil {
		return types.DealResponse{}, err
	}
	return dr, nil
}

func (d *DealStream) WriteDealResponse(dr types.DealResponse) error {
	return cborutil.WriteCborRPC(d.rw, &dr)
}

func (d *DealStream) Close() error {
	return d.rw.Close()
}

func (d *DealStream) RemotePeer() peer.ID {
	return d.p
}
