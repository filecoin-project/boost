package build

var CurrentCommit string

const BuildVersion = "0.0.1"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
