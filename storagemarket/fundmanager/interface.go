package fundmanager

type Manager interface {
	ReserveFunds() error
	ReleaseFunds() error
}
