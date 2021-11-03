package dealcheckpoints

import "golang.org/x/xerrors"

type Checkpoint int

const (
	New Checkpoint = iota
	Transferred
	Published
	PublishConfirmed
	AddedPiece
	Complete
)

var names = map[Checkpoint]string{
	New:              "New",
	Transferred:      "Transferred",
	Published:        "Published",
	PublishConfirmed: "PublishConfirmed",
	AddedPiece:       "AddedPiece",
	Complete:         "Complete",
}

var strToCP map[string]Checkpoint

func init() {
	strToCP = make(map[string]Checkpoint, len(names))
	for cp, str := range names {
		strToCP[str] = cp
	}
}

func (c Checkpoint) String() string {
	return names[c]
}

func FromString(str string) (Checkpoint, error) {
	cp, ok := strToCP[str]
	if !ok {
		return New, xerrors.Errorf("unrecognized checkpoint %s", str)
	}
	return cp, nil
}
