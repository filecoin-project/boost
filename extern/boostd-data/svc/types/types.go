package types

import "context"

type ServiceImpl interface {
	Start(ctx context.Context) error
}
