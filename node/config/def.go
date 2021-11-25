package config

import (
	"encoding"
	"os"
	"strconv"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/lotus/chain/types"
)

const (
	// RetrievalPricingDefault configures the node to use the default retrieval pricing policy.
	RetrievalPricingDefaultMode = "default"
	// RetrievalPricingExternal configures the node to use the external retrieval pricing script
	// configured by the user.
	RetrievalPricingExternalMode = "external"
)

// MaxTraversalLinks configures the maximum number of links to traverse in a DAG while calculating
// CommP and traversing a DAG with graphsync; invokes a budget on DAG depth and density.
var MaxTraversalLinks uint64 = 32 * (1 << 20)

func init() {
	if envMaxTraversal, err := strconv.ParseUint(os.Getenv("LOTUS_MAX_TRAVERSAL_LINKS"), 10, 64); err == nil {
		MaxTraversalLinks = envMaxTraversal
	}
}

func defCommon() Common {
	return Common{
		API: API{
			ListenAddress: "/ip4/127.0.0.1/tcp/1288/http",
			Timeout:       Duration(30 * time.Second),
		},
		Libp2p: Libp2p{
			ListenAddresses: []string{
				"/ip4/0.0.0.0/tcp/0",
				"/ip6/::/tcp/0",
			},
			AnnounceAddresses:   []string{},
			NoAnnounceAddresses: []string{},

			ConnMgrLow:   150,
			ConnMgrHigh:  180,
			ConnMgrGrace: Duration(20 * time.Second),
		},
	}

}

var DefaultDefaultMaxFee = types.MustParseFIL("0.07")
var DefaultSimultaneousTransfers = uint64(20)

func DefaultBoost() *Boost {
	cfg := &Boost{
		Common: defCommon(),

		Dealmaking: DealmakingConfig{
			ConsiderOnlineStorageDeals:     true,
			ConsiderOfflineStorageDeals:    true,
			ConsiderOnlineRetrievalDeals:   true,
			ConsiderOfflineRetrievalDeals:  true,
			ConsiderVerifiedStorageDeals:   true,
			ConsiderUnverifiedStorageDeals: true,
			PieceCidBlocklist:              []cid.Cid{},
			// TODO: It'd be nice to set this based on sector size
			MaxDealStartDelay:               Duration(time.Hour * 24 * 14),
			ExpectedSealDuration:            Duration(time.Hour * 24),
			PublishMsgPeriod:                Duration(time.Hour),
			PublishMsgMaxDealsPerMsg:        8,
			PublishMsgMaxFee:                types.MustParseFIL("0.05"),
			MaxProviderCollateralMultiplier: 2,

			SimultaneousTransfersForStorage:   DefaultSimultaneousTransfers,
			SimultaneousTransfersForRetrieval: DefaultSimultaneousTransfers,

			StartEpochSealingBuffer: 480, // 480 epochs buffer == 4 hours from adding deal to sector to sector being sealed

			RetrievalPricing: &RetrievalPricing{
				Strategy: RetrievalPricingDefaultMode,
				Default: &RetrievalPricingDefault{
					VerifiedDealsFreeTransfer: true,
				},
				External: &RetrievalPricingExternal{
					Path: "",
				},
			},
		},

		DAGStore: DAGStoreConfig{
			MaxConcurrentIndex:         5,
			MaxConcurrencyStorageCalls: 100,
			GCInterval:                 Duration(1 * time.Minute),
		},
	}
	return cfg
}

var _ encoding.TextMarshaler = (*Duration)(nil)
var _ encoding.TextUnmarshaler = (*Duration)(nil)

// Duration is a wrapper type for time.Duration
// for decoding and encoding from/to TOML
type Duration time.Duration

// UnmarshalText implements interface for TOML decoding
func (dur *Duration) UnmarshalText(text []byte) error {
	d, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*dur = Duration(d)
	return err
}

func (dur Duration) MarshalText() ([]byte, error) {
	d := time.Duration(dur)
	return []byte(d.String()), nil
}
