package build

var CurrentCommit string

const BuildVersion = "1.5.2"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
