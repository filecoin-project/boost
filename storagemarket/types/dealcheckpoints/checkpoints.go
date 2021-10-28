package dealcheckpoints

type Checkpoint int

const (
	New Checkpoint = iota
	Transferred
	Published
	PublishConfirmed
	AddedPiece
)
