package filters

//go:generate go run github.com/golang/mock/mockgen -destination=mock/filters.go -package=mock . Filter

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// UpdateInterval is the default interval at which the public list is refected and updated
const UpdateInterval = 5 * time.Minute

// Fetcher is a function that fetches from a remote source
// The first return value indicates whether any update has occurred since the last fetch time
// The second return is a stream of data if an update has occurred
// The third is any error
type Fetcher func(lastFetchTime time.Time) (bool, io.ReadCloser, error)

const expectedListGrowth = 128

// FetcherForHTTPEndpoint makes an fetcher that reads from an HTTP endpoint
func FetcherForHTTPEndpoint(endpoint string, authHeader string) Fetcher {
	return func(ifModifiedSince time.Time) (bool, io.ReadCloser, error) {
		// Allow empty badbits list
		if endpoint == "" {
			return false, nil, nil
		}
		req, err := http.NewRequest("GET", endpoint, nil)
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		if err != nil {
			return false, nil, err
		}
		// set the modification sync header, assuming we are not given time zero
		if !ifModifiedSince.IsZero() {
			req.Header.Set("If-Modified-Since", ifModifiedSince.Format(http.TimeFormat))
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return false, nil, err
		}
		if response.StatusCode == http.StatusNotModified {
			return false, nil, nil
		}
		if response.StatusCode < 200 && response.StatusCode > 299 {
			bodyText, _ := io.ReadAll(response.Body)
			return false, nil, fmt.Errorf("expected HTTP success code, got: %s, response body: %s", http.StatusText(response.StatusCode), string(bodyText))
		}
		return true, response.Body, nil
	}
}

type Filter interface {
	// FulfillRequest returns true if a request should be fulfilled
	// error indicates an error in processing
	FulfillRequest(p peer.ID, c cid.Cid) (bool, error)
}

type Handler interface {
	ParseUpdate(io.Reader) error
	Filter
}

type FilterDefinition struct {
	Fetcher   Fetcher
	Handler   Handler
	CacheFile string
}

type filter struct {
	FilterDefinition
	lastUpdated time.Time
}

// update updates a filter from an endpoint
func (f *filter) update() error {
	if f.Fetcher == nil {
		// if there's no fetcher, there's no update
		return nil
	}
	fetchTime := time.Now()
	updated, stream, err := f.Fetcher(f.lastUpdated)
	if err != nil {
		return fmt.Errorf("fetching endpoint: %w", err)

	}
	if !updated {
		return nil
	}
	defer func() {
		_ = stream.Close()
	}()
	// write cache to the temp file first
	tmpFile := f.CacheFile + ".tmp"
	cache, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("opening cache file: %w", err)
	}
	defer func() {
		// rename the temp file only if the operation succeeds
		if cache.Close() == nil && err == nil {
			_ = os.Rename(tmpFile, f.CacheFile)
		} else {
			_ = os.Remove(tmpFile)
		}
	}()
	forkedStream := io.TeeReader(stream, cache)
	f.lastUpdated = fetchTime
	err = f.Handler.ParseUpdate(forkedStream)
	if err != nil {
		return fmt.Errorf("parsing endpoint update: %w", err)
	}
	return nil
}

type MultiFilter struct {
	cfgDir  string
	filters []*filter
	clock   clock.Clock
	onTick  func()
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewMultiFilterWithConfigs(cfgDir string, filterDefinitions []FilterDefinition, clock clock.Clock, onTick func()) *MultiFilter {
	filters := make([]*filter, 0, len(filterDefinitions))
	for _, filterDefinition := range filterDefinitions {
		filters = append(filters, &filter{FilterDefinition: filterDefinition})
	}
	return &MultiFilter{
		cfgDir:  cfgDir,
		filters: filters,
		clock:   clock,
		onTick:  onTick,
	}
}

func NewMultiFilter(
	cfgDir string,
	apiFilterEndpoint string,
	apiFilterAuth string,
	BadBitsDenyList []string,
) *MultiFilter {
	var filters []FilterDefinition
	if len(BadBitsDenyList) > 0 {
		for i, f := range BadBitsDenyList {
			filters = append(filters, FilterDefinition{
				CacheFile: filepath.Join(cfgDir, "denylist"+strconv.Itoa(i)+".json"),
				Fetcher:   FetcherForHTTPEndpoint(f, ""),
				Handler:   NewBlockFilter(),
			})
		}
	}
	var configFetcher Fetcher
	if apiFilterEndpoint != "" {
		configFetcher = FetcherForHTTPEndpoint(apiFilterEndpoint, apiFilterAuth)
	}
	filters = append(filters, FilterDefinition{
		CacheFile: filepath.Join(cfgDir, "retrievalconfig.json"),
		Fetcher:   configFetcher,
		Handler:   NewConfigFilter(),
	})
	return NewMultiFilterWithConfigs(cfgDir, filters, clock.New(), nil)
}

// Start initializes asynchronous updates to the filter configs
// It blocks to confirm at least one synchronous update of each filter
func (mf *MultiFilter) Start(ctx context.Context) error {
	mf.ctx, mf.cancel = context.WithCancel(ctx)
	var cachedCopies []bool
	for _, f := range mf.filters {
		// open the cache file if it eixsts
		cache, err := os.Open(f.CacheFile)
		// if the file does not exist, synchronously fetch the list
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("fetching filter list: %w", err)
			}
			err = f.update()
			if err != nil {
				return err
			}
			cachedCopies = append(cachedCopies, false)
		} else {
			defer func() {
				_ = cache.Close()
			}()
			// otherwise, read the file and fetch the list asynchronously
			err = f.Handler.ParseUpdate(cache)
			if err != nil {
				return err
			}
			cachedCopies = append(cachedCopies, true)
		}
	}
	go mf.run(cachedCopies)
	return nil
}

// Close shuts down asynchronous updating
func (mf *MultiFilter) Close() {
	mf.cancel()
}

// FulfillRequest returns true if a request should be fulfilled
// error indicates an error in processing
func (mf *MultiFilter) FulfillRequest(p peer.ID, c cid.Cid) (bool, error) {
	for _, f := range mf.filters {
		has, err := f.Handler.FulfillRequest(p, c)
		if !has || err != nil {
			return has, err
		}
	}
	return true, nil
}

// run periodically updates the deny list asynchronously
func (mf *MultiFilter) run(cachedCopies []bool) {
	// if there was a cached copy, immediately asynchornously fetch an update
	for i, f := range mf.filters {
		if cachedCopies[i] {
			err := f.update()
			if err != nil {
				log.Error(err.Error())
			}
		}
	}
	updater := mf.clock.Ticker(UpdateInterval)
	defer updater.Stop()
	// call the callback if set
	if mf.onTick != nil {
		mf.onTick()
	}
	for {
		select {
		case <-mf.ctx.Done():
			return
		case <-updater.C:
			// when timer expires, update filters
			for _, f := range mf.filters {
				err := f.update()
				if err != nil {
					log.Error(err.Error())
				}
			}
			// call the callback if set
			if mf.onTick != nil {
				mf.onTick()
			}
		}
	}
}
