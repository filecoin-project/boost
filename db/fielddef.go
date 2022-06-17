package db

import (
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v8/market"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type fieldDefinition interface {
	fieldPtr() interface{}
	marshall() (interface{}, error)
	unmarshall() error
}

type fieldDef struct {
	f interface{}
}

var _ fieldDefinition = (*fieldDef)(nil)

func (fd *fieldDef) fieldPtr() interface{} {
	return fd.f
}

func (fd *fieldDef) marshall() (interface{}, error) {
	return fd.f, nil
}

func (fd *fieldDef) unmarshall() error {
	return nil
}

type cidFieldDef struct {
	cidStr sql.NullString
	f      *cid.Cid
}

func (fd *cidFieldDef) fieldPtr() interface{} {
	return &fd.cidStr
}

func (fd *cidFieldDef) marshall() (interface{}, error) {
	if fd.f == nil {
		return nil, nil
	}
	return fd.f.String(), nil
}

func (fd *cidFieldDef) unmarshall() error {
	if !fd.cidStr.Valid {
		return nil
	}

	c, err := cid.Parse(fd.cidStr.String)
	if err != nil {
		return fmt.Errorf("parsing CID from string '%s': %w", fd.cidStr.String, err)
	}

	*fd.f = c
	return nil
}

type cidPtrFieldDef struct {
	cidStr sql.NullString
	f      **cid.Cid
}

func (fd *cidPtrFieldDef) fieldPtr() interface{} {
	return &fd.cidStr
}

func (fd *cidPtrFieldDef) marshall() (interface{}, error) {
	if (*fd.f) == nil {
		return nil, nil
	}
	return (*fd.f).String(), nil
}

func (fd *cidPtrFieldDef) unmarshall() error {
	if !fd.cidStr.Valid {
		return nil
	}

	c, err := cid.Parse(fd.cidStr.String)
	if err != nil {
		return fmt.Errorf("parsing CID from string '%s': %w", fd.cidStr.String, err)
	}

	*fd.f = &c
	return nil
}

type peerIDFieldDef struct {
	marshalled sql.NullString
	f          *peer.ID
}

func (fd *peerIDFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *peerIDFieldDef) marshall() (interface{}, error) {
	if fd.f == nil {
		return nil, nil
	}
	return fd.f.String(), nil
}

func (fd *peerIDFieldDef) unmarshall() error {
	if !fd.marshalled.Valid {
		return nil
	}

	if fd.marshalled.String == "" {
		*fd.f = ""
		return nil
	}

	// The dummydeal command creates deals with a peer ID of "dummy"
	if fd.marshalled.String == "dummy" {
		*fd.f = peer.ID(fd.marshalled.String)
		return nil
	}

	var pid peer.ID
	bz := []byte(fd.marshalled.String)
	err := pid.UnmarshalText(bz)
	if err != nil {
		return fmt.Errorf("parsing peer ID from string '%s': %w", fd.marshalled.String, err)
	}

	*fd.f = pid
	return nil
}

type labelFieldDef struct {
	marshalled sql.NullString
	f          *market.DealLabel
}

func (fd *labelFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *labelFieldDef) marshall() (interface{}, error) {
	if fd.f == nil {
		return nil, nil
	}

	// If the deal label is a string, add a ' character at the beginning
	if fd.f.IsString() {
		s, err := fd.f.ToString()
		if err != nil {
			return nil, fmt.Errorf("marshalling deal label as string: %w", err)
		}
		return "'" + s, nil
	}

	// The deal label is a byte array, so hex-encode the data and add an 'x' at
	// the beginning
	bz, err := fd.f.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("marshalling deal label as bytes: %w", err)
	}
	return "x" + hex.EncodeToString(bz), nil
}

func (fd *labelFieldDef) unmarshall() error {
	if !fd.marshalled.Valid {
		return nil
	}

	if fd.marshalled.String == "" || fd.marshalled.String == "'" {
		*fd.f = market.EmptyDealLabel
		return nil
	}

	// If the first character is 'x' it's a hex-encoded byte array
	if fd.marshalled.String[0] == 'x' {
		if len(fd.marshalled.String) == 1 {
			return fmt.Errorf("cannot unmarshall empty string to hex")
		}
		bz, err := hex.DecodeString(fd.marshalled.String[1:])
		if err != nil {
			return fmt.Errorf("unmarshalling hex string %s into bytes: %w", fd.marshalled.String, err)
		}
		l, err := market.NewLabelFromBytes(bz)
		if err != nil {
			return fmt.Errorf("unmarshalling '%s' into label: %w", fd.marshalled.String, err)
		}
		*fd.f = l
		return nil
	}

	// It's a string prefixed by the ' character
	l, err := market.NewLabelFromString(fd.marshalled.String[1:])
	if err != nil {
		return fmt.Errorf("unmarshalling '%s' into label: %w", fd.marshalled.String, err)
	}

	*fd.f = l
	return nil
}

type bigIntFieldDef struct {
	marshalled sql.NullString
	f          *big.Int
}

func (fd *bigIntFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *bigIntFieldDef) marshall() (interface{}, error) {
	if fd.f == nil {
		return nil, nil
	}
	return fd.f.String(), nil
}

func (fd *bigIntFieldDef) unmarshall() error {
	if !fd.marshalled.Valid {
		*fd.f = big.NewInt(0)
		return nil
	}

	i := big.NewInt(0)
	i.SetString(fd.marshalled.String, 0)
	*fd.f = i
	return nil
}

type addrFieldDef struct {
	marshalled string
	f          *address.Address
}

func (fd *addrFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *addrFieldDef) marshall() (interface{}, error) {
	return fd.f.String(), nil
}

func (fd *addrFieldDef) unmarshall() error {
	addr, err := address.NewFromString(fd.marshalled)
	if err != nil {
		return fmt.Errorf("parsing address: %w", err)
	}

	*fd.f = addr
	return nil
}

type sigFieldDef struct {
	marshalled []byte
	f          *crypto.Signature
}

func (fd *sigFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *sigFieldDef) marshall() (interface{}, error) {
	return fd.f.MarshalBinary()
}

func (fd *sigFieldDef) unmarshall() error {
	var sig crypto.Signature
	err := sig.UnmarshalBinary(fd.marshalled)
	if err != nil {
		return fmt.Errorf("parsing signature: %w", err)
	}

	*fd.f = sig
	return nil
}

type ckptFieldDef struct {
	marshalled string
	f          *dealcheckpoints.Checkpoint
}

func (fd *ckptFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *ckptFieldDef) marshall() (interface{}, error) {
	return fd.f.String(), nil
}

func (fd *ckptFieldDef) unmarshall() error {
	cp, err := dealcheckpoints.FromString(fd.marshalled)
	if err != nil {
		return fmt.Errorf("parsing checkpoint from string '%s': %w", fd.marshalled, err)
	}

	*fd.f = cp
	return nil
}

type signedPropFieldDef struct {
	marshalled string
	f          *cid.Cid
	prop       market.ClientDealProposal
}

func (fd *signedPropFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *signedPropFieldDef) marshall() (interface{}, error) {
	propnd, err := cborutil.AsIpld(&fd.prop)
	if err != nil {
		return nil, fmt.Errorf("failed to compute signed deal proposal ipld node: %w", err)
	}

	return propnd.String(), nil
}

func (fd *signedPropFieldDef) unmarshall() error {
	if fd.f == nil {
		return nil
	}

	c, err := cid.Parse(fd.marshalled)
	if err != nil {
		return fmt.Errorf("parsing CID from string '%s': %w", fd.marshalled, err)
	}

	*fd.f = c
	return nil
}
