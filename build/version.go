package build

var CurrentCommit string

const BuildVersion = "1.7.4"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
