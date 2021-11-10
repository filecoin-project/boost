package db

import (
	"database/sql"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
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
		return xerrors.Errorf("parsing CID from string '%s': %w", fd.cidStr.String, err)
	}

	*fd.f = c
	return nil
}

type bigIntFieldDef struct {
	marshalled []byte
	f          *big.Int
}

func (fd *bigIntFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *bigIntFieldDef) marshall() (interface{}, error) {
	return fd.f.Bytes()
}

func (fd *bigIntFieldDef) unmarshall() error {
	i := big.NewInt(0)
	i.SetBytes(fd.marshalled)
	*fd.f = i
	return nil
}

type addrFieldDef struct {
	marshalled []byte
	f          *address.Address
}

func (fd *addrFieldDef) fieldPtr() interface{} {
	return &fd.marshalled
}

func (fd *addrFieldDef) marshall() (interface{}, error) {
	return fd.f.Bytes(), nil
}

func (fd *addrFieldDef) unmarshall() error {
	addr, err := address.NewFromBytes(fd.marshalled)
	if err != nil {
		return xerrors.Errorf("parsing address: %w", err)
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
		return xerrors.Errorf("parsing signature: %w", err)
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
		return xerrors.Errorf("parsing checkpoint from string '%s': %w", fd.marshalled, err)
	}

	*fd.f = cp
	return nil
}
