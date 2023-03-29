package build

var CurrentCommit string

const BuildVersion = "1.6.2-rc1"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
