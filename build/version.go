package build

var CurrentCommit string

const BuildVersion = "1.7.3-rc2"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
