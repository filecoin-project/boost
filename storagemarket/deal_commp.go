package storagemarket

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/filecoin-project/boost/storagemarket/types"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-commp-utils/writer"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
)

var ErrCommpMismatch = fmt.Errorf("commp mismatch")

// Verify that the commp provided in the deal proposal matches commp calculated
// over the downloaded file
func (p *Provider) verifyCommP(deal *types.ProviderDealState) *dealMakingError {
	p.dealLogger.Infow(deal.DealUuid, "checking commP")
	pieceCid, err := p.generatePieceCommitment(deal.InboundFilePath, deal.ClientDealProposal.Proposal.PieceSize)
	if err != nil {
		err.error = fmt.Errorf("failed to generate CommP: %w", err.error)
		return err
	}

	clientPieceCid := deal.ClientDealProposal.Proposal.PieceCID
	if pieceCid != clientPieceCid {
		if deal.IsOffline {
			// Allow manual retry in case user accidentally supplied the wrong
			// file when importing an offline deal
			return &dealMakingError{
				retry: types.DealRetryManual,
				error: fmt.Errorf("commP expected=%s, actual=%s: %w", clientPieceCid, pieceCid, ErrCommpMismatch),
			}
		}
		return &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("commP expected=%s, actual=%s: %w", clientPieceCid, pieceCid, ErrCommpMismatch),
		}
	}

	return nil
}

// generatePieceCommitment generates commp either locally or remotely,
// depending on config, and pads it as necessary to match the piece size.
func (p *Provider) generatePieceCommitment(filepath string, pieceSize abi.PaddedPieceSize) (cid.Cid, *dealMakingError) {
	pi, err := generatePieceCommitment(p.ctx, p.commpCalc, p.commpThrottle, filepath, pieceSize, p.config.RemoteCommp)
	if err != nil {
		return cid.Undef, err
	}

	return pi.PieceCID, nil
}

// Throttle the number of concurrent local commp processes
type CommpThrottle chan struct{}

// reserve waits until a slot is available, or returns a context cancelled error
func (t CommpThrottle) reserve(ctx context.Context) *dealMakingError {
	select {
	case <-ctx.Done():
		return &dealMakingError{
			retry: types.DealRetryAuto,
			error: fmt.Errorf("local commp cancelled: %w", ctx.Err()),
		}
	case t <- struct{}{}:
		return nil
	}
}

func (t CommpThrottle) release() {
	<-t
}

// generatePieceCommitment generates commp either locally or remotely,
// depending on config, and pads it as necessary to match the piece size.
func generatePieceCommitment(ctx context.Context, commpCalc smtypes.CommpCalculator, throttle CommpThrottle, filepath string, pieceSize abi.PaddedPieceSize, doRemoteCommP bool) (*abi.PieceInfo, *dealMakingError) {
	// Check whether to send commp to a remote process or do it locally
	var pi *abi.PieceInfo
	if doRemoteCommP {
		var err *dealMakingError
		pi, err = remoteCommP(ctx, commpCalc, filepath)
		if err != nil {
			err.error = fmt.Errorf("performing remote commp: %w", err.error)
			return nil, err
		}
	} else {
		if cancelledErr := throttle.reserve(ctx); cancelledErr != nil {
			return nil, cancelledErr
		}
		defer throttle.release()

		var err error
		pi, err = GenerateCommPLocally(filepath)
		if err != nil {
			return nil, &dealMakingError{
				retry: types.DealRetryFatal,
				error: fmt.Errorf("performing local commp: %w", err),
			}
		}
	}

	// if the data does not fill the whole piece
	if pi.Size < pieceSize {
		// pad the data so that it fills the piece
		rawPaddedCommp, err := commp.PadCommP(
			// we know how long a pieceCid "hash" is, just blindly extract the trailing 32 bytes
			pi.PieceCID.Hash()[len(pi.PieceCID.Hash())-32:],
			uint64(pi.Size),
			uint64(pieceSize),
		)
		if err != nil {
			return nil, &dealMakingError{
				retry: types.DealRetryFatal,
				error: fmt.Errorf("failed to pad commp: %w", err),
			}
		}
		pi.PieceCID, _ = commcid.DataCommitmentV1ToCID(rawPaddedCommp)
	}

	return pi, nil
}

// remoteCommP makes an API call to the sealing service to calculate commp
func remoteCommP(ctx context.Context, commpCalc smtypes.CommpCalculator, filepath string) (*abi.PieceInfo, *dealMakingError) {
	// Open the CAR file
	rd, err := carv2.OpenReader(filepath)
	if err != nil {
		return nil, &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("failed to get CARv2 reader: %w", err),
		}
	}

	defer func() {
		if err := rd.Close(); err != nil {
			log.Warnf("failed to close CARv2 reader for %s: %w", filepath, err)
		}
	}()

	// Get the size of the CAR file
	size, err := getCarSize(filepath, rd)
	if err != nil {
		return nil, &dealMakingError{retry: types.DealRetryFatal, error: err}
	}

	// Get the data portion of the CAR file
	dataReader, err := rd.DataReader()
	if err != nil {
		return nil, &dealMakingError{
			retry: types.DealRetryManual,
			error: fmt.Errorf("getting CAR data reader to calculate commp: %w", err),
		}
	}

	// The commp calculation requires the data to be of length
	// pieceSize.Unpadded(), so add zeros until it reaches that size
	pr, numBytes := padreader.New(dataReader, uint64(size))
	log.Debugw("computing remote commp", "size", size, "padded-size", numBytes)
	pi, err := commpCalc.ComputeDataCid(ctx, numBytes, pr)
	if err != nil {
		if ctx.Err() != nil {
			return nil, &dealMakingError{
				retry: types.DealRetryAuto,
				error: fmt.Errorf("boost shutdown while making remote API call to calculate commp: %w", ctx.Err()),
			}
		}
		return nil, &dealMakingError{
			retry: types.DealRetryManual,
			error: fmt.Errorf("making remote API call to calculate commp: %w", err),
		}
	}
	return &pi, nil
}

// GenerateCommPLocally calculates commp locally
func GenerateCommPLocally(filepath string) (*abi.PieceInfo, error) {
	rd, err := carv2.OpenReader(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to get CARv2 reader: %w", err)
	}

	defer func() {
		if err := rd.Close(); err != nil {
			log.Warnf("failed to close CARv2 reader for %s: %w", filepath, err)
		}
	}()

	// dump the CARv1 payload of the CARv2 file to the Commp Writer and get back the CommP.
	w := &writer.Writer{}
	r, err := rd.DataReader()
	if err != nil {
		return nil, fmt.Errorf("getting data reader for CAR v1 from CAR v2: %w", err)
	}

	written, err := io.Copy(w, r)
	if err != nil {
		return nil, fmt.Errorf("writing to commp writer: %w", err)
	}

	// get the size of the CAR file
	size, err := getCarSize(filepath, rd)
	if err != nil {
		return nil, err
	}

	if written != size {
		return nil, fmt.Errorf("number of bytes written to CommP writer %d not equal to the CARv1 payload size %d", written, rd.Header.DataSize)
	}

	pi, err := w.Sum()
	if err != nil {
		return nil, fmt.Errorf("failed to calculate CommP: %w", err)
	}

	return &abi.PieceInfo{
		Size:     pi.PieceSize,
		PieceCID: pi.PieceCID,
	}, nil
}

func getCarSize(filepath string, rd *carv2.Reader) (int64, error) {
	var size int64
	switch rd.Version {
	case 2:
		size = int64(rd.Header.DataSize)
	case 1:
		st, err := os.Stat(filepath)
		if err != nil {
			return 0, fmt.Errorf("failed to get CARv1 file size: %w", err)
		}
		size = st.Size()
	}
	return size, nil
}
