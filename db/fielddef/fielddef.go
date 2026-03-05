package fielddef

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v13/miner"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type FieldDefinition interface {
	FieldPtr() interface{}
	Marshall() (interface{}, error)
	Unmarshall() error
}

type FieldDef struct {
	F interface{}
}

var _ FieldDefinition = (*FieldDef)(nil)

func (fd *FieldDef) FieldPtr() interface{} {
	return fd.F
}

func (fd *FieldDef) Marshall() (interface{}, error) {
	return fd.F, nil
}

func (fd *FieldDef) Unmarshall() error {
	return nil
}

type CidFieldDef struct {
	cidStr sql.NullString
	F      *cid.Cid
}

func (fd *CidFieldDef) FieldPtr() interface{} {
	return &fd.cidStr
}

func (fd *CidFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}
	return fd.F.String(), nil
}

func (fd *CidFieldDef) Unmarshall() error {
	if !fd.cidStr.Valid {
		return nil
	}

	c, err := cid.Parse(fd.cidStr.String)
	if err != nil {
		return fmt.Errorf("parsing CID from string '%s': %w", fd.cidStr.String, err)
	}

	*fd.F = c
	return nil
}

type CidPtrFieldDef struct {
	cidStr sql.NullString
	F      **cid.Cid
}

func (fd *CidPtrFieldDef) FieldPtr() interface{} {
	return &fd.cidStr
}

func (fd *CidPtrFieldDef) Marshall() (interface{}, error) {
	if (*fd.F) == nil {
		return nil, nil
	}
	return (*fd.F).String(), nil
}

func (fd *CidPtrFieldDef) Unmarshall() error {
	if !fd.cidStr.Valid {
		return nil
	}

	c, err := cid.Parse(fd.cidStr.String)
	if err != nil {
		return fmt.Errorf("parsing CID from string '%s': %w", fd.cidStr.String, err)
	}

	*fd.F = &c
	return nil
}

type PeerIDFieldDef struct {
	Marshalled sql.NullString
	F          *peer.ID
}

func (fd *PeerIDFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *PeerIDFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}
	return fd.F.String(), nil
}

func (fd *PeerIDFieldDef) Unmarshall() error {
	if !fd.Marshalled.Valid {
		return nil
	}

	if fd.Marshalled.String == "" {
		*fd.F = ""
		return nil
	}

	// The dummydeal command creates deals with a peer ID of "dummy"
	if fd.Marshalled.String == "dummy" {
		*fd.F = peer.ID(fd.Marshalled.String)
		return nil
	}

	var pid peer.ID
	bz := []byte(fd.Marshalled.String)
	err := pid.UnmarshalText(bz)
	if err != nil {
		return fmt.Errorf("parsing peer ID from string '%s': %w", fd.Marshalled.String, err)
	}

	*fd.F = pid
	return nil
}

type LabelFieldDef struct {
	Marshalled sql.NullString
	F          *market.DealLabel
}

func (fd *LabelFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *LabelFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}

	// If the deal label is a string, add a ' character at the beginning
	if fd.F.IsString() {
		s, err := fd.F.ToString()
		if err != nil {
			return nil, fmt.Errorf("marshalling deal label as string: %w", err)
		}
		return "'" + s, nil
	}

	// The deal label is a byte array, so hex-encode the data and add an 'x' at
	// the beginning
	bz, err := fd.F.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("marshalling deal label as bytes: %w", err)
	}
	return "x" + hex.EncodeToString(bz), nil
}

func (fd *LabelFieldDef) Unmarshall() error {
	if !fd.Marshalled.Valid {
		return nil
	}

	if fd.Marshalled.String == "" || fd.Marshalled.String == "'" {
		*fd.F = market.EmptyDealLabel
		return nil
	}

	// If the first character is 'x' it's a hex-encoded byte array
	if fd.Marshalled.String[0] == 'x' {
		if len(fd.Marshalled.String) == 1 {
			return fmt.Errorf("cannot unmarshall empty string to hex")
		}
		bz, err := hex.DecodeString(fd.Marshalled.String[1:])
		if err != nil {
			return fmt.Errorf("unmarshalling hex string %s into bytes: %w", fd.Marshalled.String, err)
		}
		l, err := market.NewLabelFromBytes(bz)
		if err != nil {
			return fmt.Errorf("unmarshalling '%s' into label: %w", fd.Marshalled.String, err)
		}
		*fd.F = l
		return nil
	}

	// It's a string prefixed by the ' character
	l, err := market.NewLabelFromString(fd.Marshalled.String[1:])
	if err != nil {
		return fmt.Errorf("unmarshalling '%s' into label: %w", fd.Marshalled.String, err)
	}

	*fd.F = l
	return nil
}

func LabelFieldDefMarshall(f *market.DealLabel) (interface{}, error) {
	fd := LabelFieldDef{F: f}
	return fd.Marshall()
}

func LabelFieldDefUnmarshall(marshalled sql.NullString) (*market.DealLabel, error) {
	var f market.DealLabel
	fd := LabelFieldDef{F: &f, Marshalled: marshalled}
	err := fd.Unmarshall()
	if err != nil {
		return nil, err
	}
	return &f, nil
}

type BigIntFieldDef struct {
	Marshalled sql.NullString
	F          *big.Int
}

func (fd *BigIntFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *BigIntFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}
	return fd.F.String(), nil
}

func (fd *BigIntFieldDef) Unmarshall() error {
	if !fd.Marshalled.Valid {
		*fd.F = big.NewInt(0)
		return nil
	}

	i := big.NewInt(0)
	i.SetString(fd.Marshalled.String, 0)
	*fd.F = i
	return nil
}

type AddrFieldDef struct {
	Marshalled string
	F          *address.Address
}

func (fd *AddrFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *AddrFieldDef) Marshall() (interface{}, error) {
	return fd.F.String(), nil
}

func (fd *AddrFieldDef) Unmarshall() error {
	addr, err := address.NewFromString(fd.Marshalled)
	if err != nil {
		return fmt.Errorf("parsing address: %w", err)
	}

	*fd.F = addr
	return nil
}

type SigFieldDef struct {
	Marshalled []byte
	F          *crypto.Signature
}

func (fd *SigFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *SigFieldDef) Marshall() (interface{}, error) {
	return fd.F.MarshalBinary()
}

func (fd *SigFieldDef) Unmarshall() error {
	var sig crypto.Signature
	err := sig.UnmarshalBinary(fd.Marshalled)
	if err != nil {
		return fmt.Errorf("parsing signature: %w", err)
	}

	*fd.F = sig
	return nil
}

type CkptFieldDef struct {
	Marshalled string
	F          *dealcheckpoints.Checkpoint
}

func (fd *CkptFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *CkptFieldDef) Marshall() (interface{}, error) {
	return fd.F.String(), nil
}

func (fd *CkptFieldDef) Unmarshall() error {
	cp, err := dealcheckpoints.FromString(fd.Marshalled)
	if err != nil {
		return fmt.Errorf("parsing checkpoint from string '%s': %w", fd.Marshalled, err)
	}

	*fd.F = cp
	return nil
}

type SignedPropFieldDef struct {
	Marshalled string
	F          *cid.Cid
	Prop       market.ClientDealProposal
}

func (fd *SignedPropFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *SignedPropFieldDef) Marshall() (interface{}, error) {
	propnd, err := cborutil.AsIpld(&fd.Prop)
	if err != nil {
		return nil, fmt.Errorf("failed to compute signed deal proposal ipld node: %w", err)
	}

	return propnd.String(), nil
}

func (fd *SignedPropFieldDef) Unmarshall() error {
	if fd.F == nil {
		return nil
	}

	c, err := cid.Parse(fd.Marshalled)
	if err != nil {
		return fmt.Errorf("parsing CID from string '%s': %w", fd.Marshalled, err)
	}

	*fd.F = c
	return nil
}

type DDONotificationsFieldDef struct {
	Marshalled string
	F          []miner.DataActivationNotification
}

func (fd *DDONotificationsFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *DDONotificationsFieldDef) Marshall() (interface{}, error) {
	b, err := cborutil.Dump(fd.F)
	if err != nil {
		return nil, fmt.Errorf("failed to cbor dump DataActivationNotification: %w", err)
	}
	hexStr := fmt.Sprintf("%x", b)
	return hexStr, nil
}

func (fd *DDONotificationsFieldDef) Unmarshall() error {
	b, err := hex.DecodeString(fd.Marshalled)
	if err != nil {
		return fmt.Errorf("failed to decode hex string for DataActivationNotification: %w", err)
	}
	var out []miner.DataActivationNotification
	if err := cborutil.ReadCborRPC(bytes.NewReader(b), &out); err != nil {
		return fmt.Errorf("failed to cbor unmarshal DataActivationNotification: %w", err)
	}
	fd.F = out
	return nil
}
