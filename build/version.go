package build

var CurrentCommit string

const BuildVersion = "1.6.4"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
