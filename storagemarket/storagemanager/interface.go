package storagemanager

import "github.com/google/uuid"

type Manager interface {
	ReserveSpace(dealUuid uuid.UUID, spaceInBytes uint64) (bool, error)
	ReleaseSpace(dealUuid uuid.UUID) (bool, error)
}
