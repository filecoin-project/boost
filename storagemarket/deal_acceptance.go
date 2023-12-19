package storagemarket

import (
	"errors"
	"fmt"

	cborutil "github.com/filecoin-project/go-cbor-util"

	"github.com/filecoin-project/boost/markets/utils"
	"github.com/filecoin-project/boost/storagemarket/types"
	ctypes "github.com/filecoin-project/lotus/chain/types"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v12/market"
	"github.com/filecoin-project/go-state-types/builtin/v12/miner"
)

const DealMaxLabelSize = 256

type validationError struct {
	error
	// The reason sent to the client for why validation failed
	reason string
}

// ValidateDealProposal validates a proposed deal against the provider criteria.
// It returns a validationError. If a nicer error message should be sent to the
// client, the reason string will be set to that nicer error message.
func (p *Provider) validateDealProposal(deal types.ProviderDealState) *validationError {
	head, err := p.fullnodeApi.ChainHead(p.ctx)
	if err != nil {
		return &validationError{
			reason: "server error: getting chain head",
			error:  fmt.Errorf("node error getting most recent state id: %w", err),
		}
	}

	tok := head.Key().Bytes()
	curEpoch := head.Height()

	// Check that the proposal piece cid is defined before attempting signature
	// validation - if it's not defined, it won't be possible to marshall the
	// deal proposal to check the signature
	proposal := deal.ClientDealProposal.Proposal
	if !proposal.PieceCID.Defined() {
		return &validationError{error: fmt.Errorf("proposal PieceCID undefined")}
	}

	if ok, err := p.validateSignature(deal); err != nil || !ok {
		if err != nil {
			return &validationError{
				reason: "server error: validating signature",
				error:  fmt.Errorf("validateSignature failed: %w", err),
			}
		}
		return &validationError{
			reason: "invalid signature",
			error:  fmt.Errorf("invalid signature"),
		}
	}

	// validate deal proposal
	if proposal.Provider != p.Address {
		err := fmt.Errorf("incorrect provider for deal; proposal.Provider: %s; provider.Address: %s", proposal.Provider, p.Address)
		return &validationError{error: err}
	}

	if proposal.Label.Length() > DealMaxLabelSize {
		err := fmt.Errorf("deal label can be at most %d bytes, is %d", DealMaxLabelSize, proposal.Label.Length())
		return &validationError{error: err}
	}

	if err := proposal.PieceSize.Validate(); err != nil {
		err := fmt.Errorf("proposal piece size is invalid: %w", err)
		return &validationError{error: err}
	}

	if proposal.PieceCID.Prefix() != market.PieceCIDPrefix {
		err := fmt.Errorf("proposal PieceCID had wrong prefix")
		return &validationError{error: err}
	}

	if proposal.EndEpoch <= proposal.StartEpoch {
		err := fmt.Errorf("proposal end %d before proposal start %d", proposal.EndEpoch, proposal.StartEpoch)
		return &validationError{error: err}
	}

	if curEpoch > proposal.StartEpoch {
		err := fmt.Errorf("deal start epoch %d has already elapsed (current epoch: %d)", proposal.StartEpoch, curEpoch)
		return &validationError{error: err}
	}

	// Check that the delta between the start and end epochs (the deal
	// duration) is within acceptable bounds
	minDuration, maxDuration := market.DealDurationBounds(proposal.PieceSize)
	if proposal.Duration() < minDuration || proposal.Duration() > maxDuration {
		err := fmt.Errorf("deal duration out of bounds (min, max, provided): %d, %d, %d", minDuration, maxDuration, proposal.Duration())
		return &validationError{error: err}
	}

	// Check that the proposed end epoch isn't too far beyond the current epoch
	maxEndEpoch := curEpoch + miner.MaxSectorExpirationExtension
	if proposal.EndEpoch > maxEndEpoch {
		err := fmt.Errorf("invalid deal end epoch %d: cannot be more than %d past current epoch %d", proposal.EndEpoch, miner.MaxSectorExpirationExtension, curEpoch)
		return &validationError{error: err}
	}

	bounds, err := p.fullnodeApi.StateDealProviderCollateralBounds(p.ctx, proposal.PieceSize, proposal.VerifiedDeal, ctypes.EmptyTSK)
	if err != nil {
		return &validationError{
			reason: "server error: getting collateral bounds",
			error:  fmt.Errorf("node error getting collateral bounds: %w", err),
		}
	}

	// The maximum amount of collateral that the provider will put into escrow
	// for a deal is calculated as a multiple of the minimum bounded amount
	max := ctypes.BigMul(bounds.Min, ctypes.NewInt(p.maxDealCollateralMultiplier))

	pcMin := bounds.Min
	pcMax := max

	if proposal.ProviderCollateral.LessThan(pcMin) {
		err := fmt.Errorf("proposed provider collateral %s below minimum %s", proposal.ProviderCollateral, pcMin)
		return &validationError{error: err}
	}

	if proposal.ProviderCollateral.GreaterThan(pcMax) {
		err := fmt.Errorf("proposed provider collateral %s above maximum %s", proposal.ProviderCollateral, pcMax)
		return &validationError{error: err}
	}

	if err := p.validateAsk(deal); err != nil {
		return &validationError{error: err}
	}

	tsk, err := ctypes.TipSetKeyFromBytes(tok)
	if err != nil {
		return &validationError{
			reason: "server error: tip set key from bytes",
			error:  err,
		}
	}

	bal, err := p.fullnodeApi.StateMarketBalance(p.ctx, proposal.Client, tsk)
	if err != nil {
		return &validationError{
			reason: "server error: getting market balance",
			error:  fmt.Errorf("node error getting client market balance failed: %w", err),
		}
	}

	clientMarketBalance := utils.ToSharedBalance(bal)

	// This doesn't guarantee that the client won't withdraw / lock those funds
	// but it's a decent first filter
	if clientMarketBalance.Available.LessThan(proposal.ClientBalanceRequirement()) {
		err := fmt.Errorf("client available funds in escrow %d not enough to meet storage cost for deal %d", clientMarketBalance.Available, proposal.ClientBalanceRequirement())
		return &validationError{error: err}
	}

	// Verified deal checks
	if proposal.VerifiedDeal {
		// Get data cap
		dataCap, err := p.fullnodeApi.StateVerifiedClientStatus(p.ctx, proposal.Client, tsk)
		if err != nil {
			return &validationError{
				reason: "server error: getting verified datacap",
				error:  fmt.Errorf("node error fetching verified data cap: %w", err),
			}
		}

		if dataCap == nil {
			return &validationError{
				reason: "client is not a verified client",
				error:  errors.New("node error fetching verified data cap: data cap missing -- client not verified"),
			}
		}

		pieceSize := big.NewIntUnsigned(uint64(proposal.PieceSize))
		if dataCap.LessThan(pieceSize) {
			err := fmt.Errorf("verified deal DataCap %d too small for proposed piece size %d", dataCap, pieceSize)
			return &validationError{error: err}
		}
	}

	return nil
}

func (p *Provider) validateAsk(deal types.ProviderDealState) error {
	ask := p.GetAsk().Ask
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

func (p *Provider) validateSignature(deal types.ProviderDealState) (bool, error) {
	b, err := cborutil.Dump(&deal.ClientDealProposal.Proposal)
	if err != nil {
		return false, fmt.Errorf("failed to serialize client deal proposal: %w", err)
	}

	verified, err := p.sigVerifier.VerifySignature(p.ctx, deal.ClientDealProposal.ClientSignature, deal.ClientDealProposal.Proposal.Client, b)
	if err != nil {
		return false, fmt.Errorf("error verifying signature: %w", err)
	}
	return verified, nil
}
