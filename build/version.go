package build

var CurrentCommit string

const BuildVersion = "1.1.1"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
