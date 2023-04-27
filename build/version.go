package build

var CurrentCommit string

const BuildVersion = "1.7.2"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
