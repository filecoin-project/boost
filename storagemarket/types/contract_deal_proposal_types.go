package types

//go:generate cbor-gen-for ContractParamsVersion1

type ContractParamsVersion1 struct {
	LocationRef        string
	CarSize            uint64
	SkipIpniAnnounce   bool
	RemoveUnsealedCopy bool
}
