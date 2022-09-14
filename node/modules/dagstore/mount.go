package dagstore

import (
	"context"
	"net/url"

	"github.com/filecoin-project/dagstore/mount"
	mktsdagstore "github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

// LotusMount is a DAGStore mount implementation that fetches deal data
// from a PieceCID.
type LotusMount struct {
	API      mktsdagstore.MinerAPI
	PieceCid cid.Cid
}

func NewLotusMount(pieceCid cid.Cid, api mktsdagstore.MinerAPI) (*LotusMount, error) {
	return &LotusMount{
		PieceCid: pieceCid,
		API:      api,
	}, nil
}

func (l *LotusMount) Serialize() *url.URL {
	return &url.URL{
		Host: l.PieceCid.String(),
	}
}

func (l *LotusMount) Deserialize(u *url.URL) error {
	pieceCid, err := cid.Decode(u.Host)
	if err != nil {
		return xerrors.Errorf("failed to parse PieceCid from host '%s': %w", u.Host, err)
	}
	l.PieceCid = pieceCid
	return nil
}

func (l *LotusMount) Fetch(ctx context.Context) (mount.Reader, error) {
	return l.API.FetchUnsealedPiece(ctx, l.PieceCid)
}

func (l *LotusMount) Info() mount.Info {
	return mount.Info{
		Kind:             mount.KindRemote,
		AccessSequential: true,
		AccessSeek:       true,
		AccessRandom:     true,
	}
}

func (l *LotusMount) Close() error {
	return nil
}

func (l *LotusMount) Stat(ctx context.Context) (mount.Stat, error) {
	size, err := l.API.GetUnpaddedCARSize(ctx, l.PieceCid)
	if err != nil {
		return mount.Stat{}, xerrors.Errorf("failed to fetch piece size for piece %s: %w", l.PieceCid, err)
	}
	isUnsealed, err := l.API.IsUnsealed(ctx, l.PieceCid)
	if err != nil {
		return mount.Stat{}, xerrors.Errorf("failed to verify if we have the unsealed piece %s: %w", l.PieceCid, err)
	}

	// TODO Mark false when storage deal expires.
	return mount.Stat{
		Exists: true,
		Size:   int64(size),
		Ready:  isUnsealed,
	}, nil
}
