package build

var CurrentCommit string

const BuildVersion = "1.7.0-rc1"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
