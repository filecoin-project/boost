package gql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/lib/mpoolmonitor"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/consensus"
	"github.com/filecoin-project/lotus/chain/types"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type msg struct {
	SentEpoch  gqltypes.Uint64
	To         string
	From       string
	Nonce      gqltypes.Uint64
	Value      gqltypes.BigInt
	GasFeeCap  gqltypes.BigInt
	GasLimit   gqltypes.Uint64
	GasPremium gqltypes.BigInt
	Method     string
	Params     string
	BaseFee    gqltypes.BigInt
}

// query: mpool(local): [Message]
func (r *resolver) Mpool(ctx context.Context, args struct{ Local bool }) ([]*msg, error) {
	var ret []*msg
	var msgs []*mpoolmonitor.TimeStampedMsg

	ts, err := r.fullNode.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain head: %w", err)
	}

	baseFee := ts.Blocks()[0].ParentBaseFee

	if args.Local {
		msgs, err = r.mpool.PendingLocal(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		msgs, err = r.mpool.PendingAll()
		if err != nil {
			return nil, err
		}
	}

	// Convert params to human-readable and get method name
	for _, m := range msgs {
		var params string
		methodName := m.SignedMessage.Message.Method.String()
		toact, err := r.fullNode.StateGetActor(ctx, m.SignedMessage.Message.To, types.EmptyTSK)
		if err == nil {
			method, ok := consensus.NewActorRegistry().Methods[toact.Code][m.SignedMessage.Message.Method]
			if ok {
				methodName = method.Name

				params = string(m.SignedMessage.Message.Params)
				p, ok := reflect.New(method.Params.Elem()).Interface().(cbg.CBORUnmarshaler)
				if ok {
					if err := p.UnmarshalCBOR(bytes.NewReader(m.SignedMessage.Message.Params)); err == nil {
						b, err := json.MarshalIndent(p, "", "  ")
						if err == nil {
							params = string(b)
						}
					}
				}
			}
		}

		ret = append(ret, &msg{
			SentEpoch:  gqltypes.Uint64(m.Added),
			To:         m.SignedMessage.Message.To.String(),
			From:       m.SignedMessage.Message.From.String(),
			Nonce:      gqltypes.Uint64(m.SignedMessage.Message.Nonce),
			Value:      gqltypes.BigInt{Int: m.SignedMessage.Message.Value},
			GasFeeCap:  gqltypes.BigInt{Int: m.SignedMessage.Message.GasFeeCap},
			GasLimit:   gqltypes.Uint64(uint64(m.SignedMessage.Message.GasLimit)),
			GasPremium: gqltypes.BigInt{Int: m.SignedMessage.Message.GasPremium},
			Method:     methodName,
			Params:     params,
			BaseFee:    gqltypes.BigInt{Int: baseFee},
		})
	}

	return ret, nil
}

// query: mpoolAlertsCount: int
func (r *resolver) MpoolAlertsCount(ctx context.Context) (int32, error) {
	msgs, err := r.mpool.Alerts(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting alert count: %w", err)
	}
	return int32(len(msgs)), nil
}

func mockMessages() []*types.SignedMessage {
	to0, _ := address.NewFromString("f01469945")
	from0, _ := address.NewFromString("f3uakndzne4lorwykinlitx2d2puuhgburvxw4dpkfskeofmzg33pm7okyzikqe2gzvaqj2k3hpunwayij6haa")
	to1, _ := address.NewFromString("f13vk7dxblv6eslc3utzdlt3vlyc6yhdfopfxv5ay")
	from1, _ := address.NewFromString("f1oz4avehbenl4zlm4k56wlnpyeowptvi45im6w4y")
	return []*types.SignedMessage{{
		Message: types.Message{
			To:         to0,
			From:       from0,
			Nonce:      51803,
			Value:      abi.NewTokenAmount(295064561142142327),
			GasLimit:   53679021,
			GasFeeCap:  abi.NewTokenAmount(101088),
			GasPremium: abi.NewTokenAmount(100024),
			Method:     abi.MethodNum(7),
			Params:     []byte("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"),
		},
	}, {
		Message: types.Message{
			To:         to1,
			From:       from1,
			Nonce:      2,
			Value:      abi.NewTokenAmount(1810412379337683000),
			GasLimit:   2628272,
			GasFeeCap:  abi.NewTokenAmount(233844375),
			GasPremium: abi.NewTokenAmount(0),
			Method:     abi.MethodNum(0),
			Params:     nil,
		},
	}}
}

var _ = mockMessages()
