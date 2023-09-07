package frisbii

import (
	"bytes"
	"context"
	"io"

	// codecs we care about

	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-trustless-utils/traversal"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage/deferred"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
)

// StreamCar streams a DAG in CARv1 format to the given writer, using the given
// selector.
func StreamCar(
	ctx context.Context,
	requestLsys linking.LinkSystem,
	out io.Writer,
	request trustlessutils.Request,
) error {
	carWriter := deferred.NewDeferredCarWriterForStream(out, []cid.Cid{request.Root}, car.AllowDuplicatePuts(request.Duplicates))
	requestLsys.StorageReadOpener = carPipe(requestLsys.StorageReadOpener, carWriter)

	cfg := traversal.Config{Root: request.Root, Selector: request.Selector()}
	lastPath, err := cfg.Traverse(ctx, requestLsys, nil)
	if err != nil {
		return err
	}

	if err := traversal.CheckPath(datamodel.ParsePath(request.Path), lastPath); err != nil {
		logger.Warnf("failed to traverse full requested path: %s", err)
	}

	return nil
}

func carPipe(orig linking.BlockReadOpener, car *deferred.DeferredCarWriter) linking.BlockReadOpener {
	return func(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		r, err := orig(lc, lnk)
		if err != nil {
			return nil, err
		}
		byts, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		err = car.Put(lc.Ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(byts), nil
	}
}
