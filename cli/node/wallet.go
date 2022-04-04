package node

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/wallet"
)

type DealProposalSigner struct {
	*wallet.LocalWallet
}

func (fwi DealProposalSigner) WalletSign(ctx context.Context, addr address.Address, data []byte) (*crypto.Signature, error) {
	return fwi.LocalWallet.WalletSign(ctx, addr, data, api.MsgMeta{Type: api.MTDealProposal})
}
