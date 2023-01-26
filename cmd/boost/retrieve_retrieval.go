package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/boost/retrieve"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipld/go-ipld-prime"
	"golang.org/x/term"
	"golang.org/x/xerrors"
)

// A retrieval attempt is a configuration for performing a specific retrieval
// over a specific network
type RetrievalAttempt interface {
	Retrieve(context.Context, *Node) (RetrievalStats, error)
}

type IPFSRetrievalAttempt struct {
	Cid cid.Cid
}

func (attempt *IPFSRetrievalAttempt) Retrieve(ctx context.Context, node *Node) (RetrievalStats, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Info("Searching IPFS for CID...")

	providers := node.DHT.FindProvidersAsync(ctx, attempt.Cid, 0)

	// Ready will be true if we connected to at least one provider, false if no
	// miners successfully connected
	ready := make(chan bool, 1)
	go func() {
		for {
			select {
			case provider, ok := <-providers:
				if !ok {
					ready <- false
					return
				}

				// If no addresses are listed for the provider, we should just
				// skip it
				if len(provider.Addrs) == 0 {
					log.Debugf("Skipping IPFS provider with no addresses %s", provider.ID)
					continue
				}

				log.Infof("Connected to IPFS provider %s", provider.ID)
				ready <- true
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	// TODO: also add connection timeout
	case <-ctx.Done():
		return nil, ctx.Err()
	case ready := <-ready:
		if !ready {
			return nil, fmt.Errorf("couldn't find CID")
		}
	}

	// If we were able to connect to at least one of the providers, go ahead
	// with the retrieval

	var progressLk sync.Mutex
	var bytesRetrieved uint64 = 0
	startTime := time.Now()

	log.Info("Starting retrieval")

	bserv := blockservice.New(node.Blockstore, node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)
	//dsess := dserv.Session(ctx)

	cset := cid.NewSet()
	if err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipldformat.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		// Only count leaf nodes toward the total size
		if len(node.Links()) == 0 {
			progressLk.Lock()
			nodeSize, err := node.Size()
			if err != nil {
				nodeSize = 0
			}
			bytesRetrieved += nodeSize
			printProgress(bytesRetrieved)
			progressLk.Unlock()
		}

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return node.Links(), nil
	}, attempt.Cid, cset.Visit, merkledag.Concurrent()); err != nil {
		return nil, err
	}

	log.Info("IPFS retrieval succeeded")

	return &IPFSRetrievalStats{
		ByteSize: bytesRetrieved,
		Duration: time.Since(startTime),
	}, nil
}

type FILRetrievalAttempt struct {
	FilClient  *retrieve.FilClient
	Cid        cid.Cid
	Candidates []FILRetrievalCandidate
	SelNode    ipld.Node

	// Disable sorting of candidates based on preferability
	NoSort bool
}

func (attempt *FILRetrievalAttempt) Retrieve(ctx context.Context, node *Node) (RetrievalStats, error) {
	// If no miners are provided, there's nothing else we can do
	if len(attempt.Candidates) == 0 {
		log.Info("No miners were provided, will not attempt FIL retrieval")
		return nil, xerrors.Errorf("retrieval failed: no miners were provided")
	}

	// If IPFS retrieval was unavailable, do a full FIL retrieval. Start with
	// querying all the candidates for sorting.

	log.Info("Querying FIL retrieval candidates...")

	type CandidateQuery struct {
		Candidate FILRetrievalCandidate
		Response  *retrievalmarket.QueryResponse
	}
	checked := 0
	var queries []CandidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(attempt.Candidates))

	for _, candidate := range attempt.Candidates {

		// Copy into loop, cursed go
		candidate := candidate

		go func() {
			defer wg.Done()

			query, err := attempt.FilClient.RetrievalQuery(ctx, candidate.Miner, candidate.RootCid)
			if err != nil {
				log.Debugf("Retrieval query for miner %s failed: %v", candidate.Miner, err)
				return
			}

			queriesLk.Lock()
			queries = append(queries, CandidateQuery{Candidate: candidate, Response: query})
			checked++
			fmt.Fprintf(os.Stderr, "%v/%v\r", checked, len(attempt.Candidates))
			queriesLk.Unlock()
		}()
	}

	wg.Wait()

	log.Infof("Got back %v retrieval query results of a total of %v candidates", len(queries), len(attempt.Candidates))

	if len(queries) == 0 {
		return nil, xerrors.Errorf("retrieval failed: queries failed for all miners")
	}

	// After we got the query results, sort them with respect to the candidate
	// selection config as long as noSort isn't requested (TODO - more options)

	if !attempt.NoSort {
		sort.Slice(queries, func(i, j int) bool {
			a := queries[i].Response
			b := queries[i].Response

			// Always prefer unsealed to sealed, no matter what
			if a.UnsealPrice.IsZero() && !b.UnsealPrice.IsZero() {
				return true
			}

			// Select lower price, or continue if equal
			aTotalPrice := totalCost(a)
			bTotalPrice := totalCost(b)
			if !aTotalPrice.Equals(bTotalPrice) {
				return aTotalPrice.LessThan(bTotalPrice)
			}

			// Select smaller size, or continue if equal
			if a.Size != b.Size {
				return a.Size < b.Size
			}

			return false
		})
	}

	// Now attempt retrievals in serial from first to last, until one works.
	// stats will get set if a retrieval succeeds - if no retrievals work, it
	// will still be nil after the loop finishes
	var stats *FILRetrievalStats = nil
	for _, query := range queries {
		log.Infof("Attempting FIL retrieval with miner %s from root CID %s (%s)", query.Candidate.Miner, query.Candidate.RootCid, types.FIL(totalCost(query.Response)))

		if attempt.SelNode != nil && !attempt.SelNode.IsNull() {
			log.Infof("Using selector %s", attempt.SelNode)
		}

		proposal, err := retrieve.RetrievalProposalForAsk(query.Response, query.Candidate.RootCid, attempt.SelNode)
		if err != nil {
			log.Debugf("Failed to create retrieval proposal with candidate miner %s: %v", query.Candidate.Miner, err)
			continue
		}

		var bytesReceived uint64
		stats_, err := attempt.FilClient.RetrieveContentWithProgressCallback(
			ctx,
			query.Candidate.Miner,
			proposal,
			func(bytesReceived_ uint64) {
				bytesReceived = bytesReceived_
				printProgress(bytesReceived)
			},
		)
		if err != nil {
			log.Errorf("Failed to retrieve content with candidate miner %s: %v", query.Candidate.Miner, err)
			continue
		}

		stats = &FILRetrievalStats{RetrievalStats: *stats_}
		break
	}

	if stats == nil {
		return nil, xerrors.New("retrieval failed for all miners")
	}

	log.Info("FIL retrieval succeeded")

	return stats, nil
}

type FILRetrievalCandidate struct {
	Miner   address.Address
	RootCid cid.Cid
	DealID  uint
}

func (node *Node) GetRetrievalCandidates(endpoint string, c cid.Cid) ([]FILRetrievalCandidate, error) {

	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, xerrors.Errorf("endpoint %s is not a valid url", endpoint)
	}
	endpointURL.Path = path.Join(endpointURL.Path, c.String())

	resp, err := http.Get(endpointURL.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request to endpoint %s got status %v", endpointURL, resp.StatusCode)
	}

	var res []FILRetrievalCandidate

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, xerrors.Errorf("could not unmarshal http response for cid %s", c)
	}

	return res, nil
}

type RetrievalStats interface {
	GetByteSize() uint64
	GetDuration() time.Duration
	GetAverageBytesPerSecond() uint64
}

type FILRetrievalStats struct {
	retrieve.RetrievalStats
}

func (stats *FILRetrievalStats) GetByteSize() uint64 {
	return stats.Size
}

func (stats *FILRetrievalStats) GetDuration() time.Duration {
	return stats.Duration
}

func (stats *FILRetrievalStats) GetAverageBytesPerSecond() uint64 {
	return stats.AverageSpeed
}

type IPFSRetrievalStats struct {
	ByteSize uint64
	Duration time.Duration
}

func (stats *IPFSRetrievalStats) GetByteSize() uint64 {
	return stats.ByteSize
}

func (stats *IPFSRetrievalStats) GetDuration() time.Duration {
	return stats.Duration
}

func (stats *IPFSRetrievalStats) GetAverageBytesPerSecond() uint64 {
	return uint64(float64(stats.ByteSize) / stats.Duration.Seconds())
}

// Takes a list of network configs to attempt to retrieve from, in order. Valid
// structs for the interface: IPFSRetrievalConfig, FILRetrievalConfig
func (node *Node) RetrieveFromBestCandidate(
	ctx context.Context,
	attempts []RetrievalAttempt,
) (RetrievalStats, error) {
	for _, attempt := range attempts {
		stats, err := attempt.Retrieve(ctx, node)
		if err == nil {
			return stats, nil
		}
	}

	return nil, fmt.Errorf("all retrieval attempts failed")
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}

func printProgress(bytesReceived uint64) {
	str := fmt.Sprintf("%v (%v)", bytesReceived, humanize.IBytes(bytesReceived))

	termWidth, _, err := term.GetSize(int(os.Stdin.Fd()))
	strLen := len(str)
	if err == nil {

		if strLen < termWidth {
			// If the string is shorter than the terminal width, pad right side
			// with spaces to remove old text
			str = strings.Join([]string{str, strings.Repeat(" ", termWidth-strLen)}, "")
		} else if strLen > termWidth {
			// If the string doesn't fit in the terminal, cut it down to a size
			// that fits
			str = str[:termWidth]
		}
	}

	fmt.Fprintf(os.Stderr, "%s\r", str)
}

func printAskResponse(ask *storagemarket.StorageAsk) {
	fmt.Printf(`ASK RESPONSE
-----
Miner: %v
Price (Unverified): %v (%v)
Price (Verified): %v (%v)
Min Piece Size: %v
Max Piece Size: %v
`,
		ask.Miner,
		ask.Price, types.FIL(ask.Price),
		ask.VerifiedPrice, types.FIL(ask.VerifiedPrice),
		ask.MinPieceSize,
		ask.MaxPieceSize,
	)
}

func printDealStatus(state *storagemarket.ProviderDealState) {
	fmt.Printf(`DEAL STATUS
-----
Deal State:     %s
Proposal CID:   %s
Add Funds CID:  %s
Publish CID:    %s
Deal ID:        %d
Fast Retrieval: %t
`,
		storagemarket.DealStates[state.State],
		state.ProposalCid,
		state.AddFundsCid,
		state.PublishCid,
		state.DealID,
		state.FastRetrieval,
	)

	stateProposalLabel, err := state.Proposal.Label.ToString()
	if err != nil {
		fmt.Printf("Message: %s\n", state.Message)
		stateProposalLabel = ""
	}
	if state.Proposal != nil {
		fmt.Printf(`Proposal:
	Piece CID:               %s
	Piece Size:              %d (%s)
	Verified Deal:           %t
	Client:                  %s
	Provider:                %s
	Label:                   %s
	Start Epoch:             %d
	End Epoch:               %d
	Storage Price Per Epoch: %d (%s)
	Provider Collateral:     %d (%s)
	Client Collateral:       %d (%d)
`,
			state.Proposal.PieceCID,
			state.Proposal.PieceSize, humanize.IBytes(uint64(state.Proposal.PieceSize)),
			state.Proposal.VerifiedDeal,
			state.Proposal.Client,
			state.Proposal.Provider,
			stateProposalLabel,
			state.Proposal.StartEpoch,
			state.Proposal.EndEpoch,
			state.Proposal.StoragePricePerEpoch, types.FIL(state.Proposal.StoragePricePerEpoch),
			state.Proposal.ProviderCollateral, types.FIL(state.Proposal.ProviderCollateral),
			state.Proposal.ClientCollateral, types.FIL(state.Proposal.ClientCollateral),
		)
	}

	if state.Message != "" {
		fmt.Printf("Message: %s\n", state.Message)
	}
}

func printRetrievalStats(stats RetrievalStats) {
	switch stats := stats.(type) {
	case *FILRetrievalStats:
		fmt.Printf(`RETRIEVAL STATS (FIL)
-----
Size:          %v (%v)
Duration:      %v
Average Speed: %v (%v/s)
Ask Price:     %v (%v)
Total Payment: %v (%v)
Num Payments:  %v
Peer:          %v
`,
			stats.Size, humanize.IBytes(stats.Size),
			stats.Duration,
			stats.AverageSpeed, humanize.IBytes(stats.AverageSpeed),
			stats.AskPrice, types.FIL(stats.AskPrice),
			stats.TotalPayment, types.FIL(stats.TotalPayment),
			stats.NumPayments,
			stats.Peer,
		)
	case *IPFSRetrievalStats:
		fmt.Printf(`RETRIEVAL STATS (IPFS)
-----
Size:          %v (%v)
Duration:      %v
Average Speed: %v
`,
			stats.ByteSize, humanize.IBytes(stats.ByteSize),
			stats.Duration,
			stats.GetAverageBytesPerSecond(),
		)
	}
}

func printQueryResponse(query *retrievalmarket.QueryResponse, availableOnIPFS bool) {
	var status string
	switch query.Status {
	case retrievalmarket.QueryResponseAvailable:
		status = "Available"
	case retrievalmarket.QueryResponseUnavailable:
		status = "Unavailable"
	case retrievalmarket.QueryResponseError:
		status = "Error"
	default:
		status = fmt.Sprintf("Unrecognized Status (%d)", query.Status)
	}

	var pieceCIDFound string
	switch query.PieceCIDFound {
	case retrievalmarket.QueryItemAvailable:
		pieceCIDFound = "Available"
	case retrievalmarket.QueryItemUnavailable:
		pieceCIDFound = "Unavailable"
	case retrievalmarket.QueryItemUnknown:
		pieceCIDFound = "Unknown"
	default:
		pieceCIDFound = fmt.Sprintf("Unrecognized (%d)", query.PieceCIDFound)
	}

	total := big.Add(query.UnsealPrice, big.Mul(big.NewIntUnsigned(query.Size), query.MinPricePerByte))
	fmt.Printf(`QUERY RESPONSE
-----
Status:                        %v
Piece CID Found:               %v
Size:                          %v (%v)
Unseal Price:                  %v (%v)
Min Price Per Byte:            %v (%v)
Total Retrieval Price:         %v (%v)
Payment Address:               %v
Max Payment Interval:          %v (%v)
Max Payment Interval Increase: %v (%v)
`,
		status,
		pieceCIDFound,
		query.Size, humanize.IBytes(query.Size),
		query.UnsealPrice, types.FIL(query.UnsealPrice),
		query.MinPricePerByte, types.FIL(query.MinPricePerByte),
		total, types.FIL(total),
		query.PaymentAddress,
		query.MaxPaymentInterval, humanize.IBytes(query.MaxPaymentInterval),
		query.MaxPaymentIntervalIncrease, humanize.IBytes(query.MaxPaymentIntervalIncrease),
	)

	if query.Message != "" {
		fmt.Printf("Message: %v\n", query.Message)
	}

	if availableOnIPFS {
		fmt.Printf("-----\nAvaiable on IPFS")
	}
}
