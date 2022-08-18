package build

var CurrentCommit string

const BuildVersion = "1.3.0"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
