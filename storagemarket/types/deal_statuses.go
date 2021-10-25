package types

// Status is the status of a boost storage deal and represents an unit of resumption.
type Status int

const (
	New Status = iota
	Transferring
	Publishing
	Sealing
	Complete
	Failed
	Cancelled
)

var stateToString = map[Status]string{
	New:          "New",
	Transferring: "Transferring",
	Publishing:   "Publishing",
	Sealing:      "Sealing",
	Complete:     "Complete",
	Failed:       "Failed",
	Cancelled:    "Cancelled",
}

func (s Status) String() string {
	str, ok := stateToString[s]
	if !ok {
		return "__undefined__"
	}
	return str
}
