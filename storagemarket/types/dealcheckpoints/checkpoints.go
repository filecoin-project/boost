package dealcheckpoints

type Checkpoint int

const (
	New Checkpoint = iota
	Transferred
	FundsReserved
	Published
	PublishConfirmed
	AddedPiece
)
