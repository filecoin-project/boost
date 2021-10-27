package storagemarket

import (
	"errors"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types"

	"github.com/filecoin-project/go-fil-markets/shared"

	cborutil "github.com/filecoin-project/go-cbor-util"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
)

const DealMaxLabelSize = 256

// ValidateDealProposal validates a proposed deal against the provider criteria
func (p *provider) validateDealProposal(deal types.ProviderDealState) error {
	tok, curEpoch, err := p.lotusNode.GetChainHead(p.ctx)
	if err != nil {
		return fmt.Errorf("node error getting most recent state id: %w", err)
	}

	// verify client signature
	if err := p.validateSignature(tok, deal); err != nil {
		return fmt.Errorf("validateSignature failed: %w", err)
	}

	// validate deal proposal
	proposal := deal.ClientDealProposal.Proposal
	if proposal.Provider != p.Address {
		return fmt.Errorf("incorrect provider for deal")
	}

	if len(proposal.Label) > DealMaxLabelSize {
		return fmt.Errorf("deal label can be at most %d bytes, is %d", DealMaxLabelSize, len(proposal.Label))
	}

	if err := proposal.PieceSize.Validate(); err != nil {
		return fmt.Errorf("proposal piece size is invalid: %w", err)
	}

	if !proposal.PieceCID.Defined() {
		return fmt.Errorf("proposal PieceCID undefined")
	}

	if proposal.PieceCID.Prefix() != market.PieceCIDPrefix {
		return fmt.Errorf("proposal PieceCID had wrong prefix")
	}

	if proposal.EndEpoch <= proposal.StartEpoch {
		return fmt.Errorf("proposal end before proposal start")
	}

	if curEpoch > proposal.StartEpoch {
		return fmt.Errorf("deal start epoch has already elapsed")
	}

	// Check that the delta between the start and end epochs (the deal
	// duration) is within acceptable bounds
	minDuration, maxDuration := market2.DealDurationBounds(proposal.PieceSize)
	if proposal.Duration() < minDuration || proposal.Duration() > maxDuration {
		return fmt.Errorf("deal duration out of bounds (min, max, provided): %d, %d, %d", minDuration, maxDuration, proposal.Duration())
	}

	// Check that the proposed end epoch isn't too far beyond the current epoch
	maxEndEpoch := curEpoch + miner.MaxSectorExpirationExtension
	if proposal.EndEpoch > maxEndEpoch {
		return fmt.Errorf("invalid deal end epoch %d: cannot be more than %d past current epoch %d", proposal.EndEpoch, miner.MaxSectorExpirationExtension, curEpoch)
	}

	pcMin, pcMax, err := p.lotusNode.DealProviderCollateralBounds(p.ctx, proposal.PieceSize, proposal.VerifiedDeal)
	if err != nil {
		return fmt.Errorf("node error getting collateral bounds: %w", err)
	}

	if proposal.ProviderCollateral.LessThan(pcMin) {
		return fmt.Errorf("proposed provider collateral below minimum: %s < %s", proposal.ProviderCollateral, pcMin)
	}

	if proposal.ProviderCollateral.GreaterThan(pcMax) {
		return fmt.Errorf("proposed provider collateral above maximum: %s > %s", proposal.ProviderCollateral, pcMax)
	}

	if err := p.validateAsk(deal); err != nil {
		return fmt.Errorf("validateAsk failed: %w", err)
	}

	// check market funds
	clientMarketBalance, err := p.lotusNode.GetBalance(p.ctx, proposal.Client, tok)
	if err != nil {
		return fmt.Errorf("node error getting client market balance failed: %w", err)
	}

	// This doesn't guarantee that the client won't withdraw / lock those funds
	// but it's a decent first filter
	if clientMarketBalance.Available.LessThan(proposal.ClientBalanceRequirement()) {
		return fmt.Errorf("clientMarketBalance.Available too small: %d < %d", clientMarketBalance.Available, proposal.ClientBalanceRequirement())
	}

	// Verified deal checks
	if proposal.VerifiedDeal {
		dataCap, err := p.lotusNode.GetDataCap(p.ctx, proposal.Client, tok)
		if err != nil {
			return fmt.Errorf("node error fetching verified data cap: %w", err)
		}

		if dataCap == nil {
			return errors.New("node error fetching verified data cap: data cap missing -- client not verified")
		}

		pieceSize := big.NewIntUnsigned(uint64(proposal.PieceSize))
		if dataCap.LessThan(pieceSize) {
			return errors.New("verified deal DataCap too small for proposed piece size")
		}
	}

	return nil
}

func (p *provider) validateAsk(deal types.ProviderDealState) error {
	ask := p.GetAsk()
	askPrice := ask.Price
	if deal.ClientDealProposal.Proposal.VerifiedDeal {
		askPrice = ask.VerifiedPrice
	}

	proposal := deal.ClientDealProposal.Proposal
	minPrice := big.Div(big.Mul(askPrice, abi.NewTokenAmount(int64(proposal.PieceSize))), abi.NewTokenAmount(1<<30))
	if proposal.StoragePricePerEpoch.LessThan(minPrice) {
		return fmt.Errorf("storage price per epoch less than asking price: %s < %s", proposal.StoragePricePerEpoch, minPrice)
	}

	if proposal.PieceSize < ask.MinPieceSize {
		return fmt.Errorf("piece size less than minimum required size: %d < %d", proposal.PieceSize, ask.MinPieceSize)
	}

	if proposal.PieceSize > ask.MaxPieceSize {
		return fmt.Errorf("piece size more than maximum allowed size: %d > %d", proposal.PieceSize, ask.MaxPieceSize)
	}

	return nil
}

func (p *provider) validateSignature(tok shared.TipSetToken, deal types.ProviderDealState) error {
	b, err := cborutil.Dump(&deal.ClientDealProposal.Proposal)
	if err != nil {
		return fmt.Errorf("failed to serialize client deal proposal: %w", err)
	}

	verified, err := p.lotusNode.VerifySignature(p.ctx, deal.ClientDealProposal.ClientSignature, deal.ClientDealProposal.Proposal.Client, b, tok)
	if err != nil {
		return fmt.Errorf("error verifying signature: %w", err)
	}
	if !verified {
		return errors.New("could not verify signature")
	}

	return nil
}
