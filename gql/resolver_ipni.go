package gql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/multiformats/go-multicodec"
)

type ipniResolver struct {
	PeerID string
	Config string
}

func (r *resolver) IpniProviderInfo() (*ipniResolver, error) {
	cfg, err := json.Marshal(r.cfg.IndexProvider)
	if err != nil {
		return nil, err
	}

	return &ipniResolver{PeerID: r.h.ID().String(), Config: string(cfg)}, nil
}

type adMetadata struct {
	Protocol string
	Metadata string
}

type extendedProvResolver struct {
	ID        string
	Addresses []string
	Metadata  []*adMetadata
}

type extendedProvsResolver struct {
	Providers []*extendedProvResolver
	Override  bool
}

type adResolver struct {
	ContextID         string
	Entries           string
	PreviousEntry     string
	Provider          string
	Addresses         []string
	IsRemove          bool
	Metadata          []*adMetadata
	ExtendedProviders *extendedProvsResolver
}

func (r *resolver) IpniAdvertisement(ctx context.Context, args struct{ AdCid string }) (*adResolver, error) {
	adCid, err := cid.Parse(args.AdCid)
	if err != nil {
		return nil, fmt.Errorf("parsing ad cid %s: %w", args.AdCid, err)
	}

	ad, err := r.idxProv.GetAdv(ctx, adCid)
	if err != nil {
		return nil, err
	}

	return resolveAd(ad), nil
}

func getMetadata(adMdBytes []byte) []*adMetadata {
	var mds []*adMetadata
	if len(adMdBytes) == 0 {
		return nil
	}

	// Metadata may contain more than one protocol, sorted by ascending order of their protocol ID.
	// Therefore, decode the metadata as metadata.Metadata, then read each protocol's data.
	// See: https://github.com/ipni/specs/blob/main/IPNI.md#metadata
	dtm := metadata.Default.New()
	if err := dtm.UnmarshalBinary(adMdBytes); err != nil {
		for _, protoId := range dtm.Protocols() {
			adMd := &adMetadata{
				Protocol: protoId.String(),
			}
			mds = append(mds, adMd)
			if protoId == multicodec.TransportGraphsyncFilecoinv1 {
				proto := dtm.Get(protoId)
				gsmd, ok := proto.(*metadata.GraphsyncFilecoinV1)
				if ok {
					bz, err := json.Marshal(gsmd)
					if err == nil {
						adMd.Metadata = string(bz)
					} else {
						adMd.Metadata = err.Error()
					}
				}
			}
		}
	}

	return mds
}

func (r *resolver) IpniAdvertisementEntries(ctx context.Context, args struct{ AdCid string }) ([]*string, error) {
	adCid, err := cid.Parse(args.AdCid)
	if err != nil {
		return nil, fmt.Errorf("parsing ad cid %s: %w", args.AdCid, err)
	}

	ad, err := r.idxProv.GetAdv(ctx, adCid)
	if err != nil {
		return nil, err
	}

	if len(ad.ContextID) == 0 {
		return nil, nil
	}

	var entries []*string
	it, err := r.idxProvWrapper.MultihashLister(ctx, "", ad.ContextID)
	if err != nil {
		return nil, err
	}
	for entry, err := it.Next(); err == nil; entry, err = it.Next() {
		str := entry.B58String()
		entries = append(entries, &str)
	}

	return entries, nil
}

func (r *resolver) IpniAdvertisementEntriesCount(ctx context.Context, args struct{ AdCid string }) (int32, error) {
	adCid, err := cid.Parse(args.AdCid)
	if err != nil {
		return 0, fmt.Errorf("parsing ad cid %s: %w", args.AdCid, err)
	}

	ad, err := r.idxProv.GetAdv(ctx, adCid)
	if err != nil {
		return 0, err
	}

	if len(ad.ContextID) == 0 {
		return 0, nil
	}

	it, err := r.idxProvWrapper.MultihashLister(ctx, "", ad.ContextID)
	if err != nil {
		return 0, err
	}
	var count int32
	for _, err := it.Next(); err == nil; _, err = it.Next() {
		count++
	}

	return count, nil
}

func (r *resolver) IpniLatestAdvertisement(ctx context.Context) (string, error) {
	adCid, _, err := r.idxProv.GetLatestAdv(ctx)
	if err != nil {
		return "", err
	}

	return adCid.String(), nil
}

func resolveAd(ad *schema.Advertisement) *adResolver {
	var contextId string
	c, err := cid.Parse(ad.ContextID)
	if err == nil {
		contextId = c.String()
	} else {
		contextId = base64.StdEncoding.EncodeToString(ad.ContextID)
	}

	res := &adResolver{
		ContextID:     contextId,
		IsRemove:      ad.IsRm,
		Metadata:      getMetadata(ad.Metadata),
		PreviousEntry: ad.PreviousID.String(),
		Provider:      ad.Provider,
		Addresses:     ad.Addresses,
	}

	if ad.ExtendedProvider != nil {
		extProvs := &extendedProvsResolver{
			Override: ad.ExtendedProvider.Override,
		}
		for _, p := range ad.ExtendedProvider.Providers {
			extProvs.Providers = append(extProvs.Providers, &extendedProvResolver{
				ID:        p.ID,
				Addresses: p.Addresses,
				Metadata:  getMetadata(p.Metadata),
			})
		}
		res.ExtendedProviders = extProvs
	}
	return res
}

func (r *resolver) IpniDistanceFromLatestAd(ctx context.Context, args struct {
	LatestAdcid string
	Adcid       string
}) (int32, error) {
	count := int32(0)
	if args.Adcid == args.LatestAdcid {
		return count, nil
	}
	latestAd, err := cid.Parse(args.LatestAdcid)
	if err != nil {
		return count, fmt.Errorf("parsing ad cid %s: %w", args.LatestAdcid, err)
	}

	ad, err := r.idxProv.GetAdv(ctx, latestAd)
	if err != nil {
		return count, err
	}

	for ad.PreviousID.String() != args.Adcid {
		prevCid := ad.PreviousID.(cidlink.Link).Cid
		ad, err = r.idxProv.GetAdv(ctx, prevCid)
		if err != nil {
			return count, err
		}
		count++
	}

	return count, nil
}
