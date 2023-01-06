package dealcheckpoints

import "fmt"

type Checkpoint int

const (
	Accepted Checkpoint = iota
	Transferred
	Published
	PublishConfirmed
	AddedPiece
	IndexedAndAnnounced
	Complete
)

var names = map[Checkpoint]string{
	Accepted:            "Accepted",
	Transferred:         "Transferred",
	Published:           "Published",
	PublishConfirmed:    "PublishConfirmed",
	AddedPiece:          "AddedPiece",
	IndexedAndAnnounced: "IndexedAndAnnounced",
	Complete:            "Complete",
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
		return Accepted, fmt.Errorf("unrecognized checkpoint %s", str)
	}
	return cp, nil
}
