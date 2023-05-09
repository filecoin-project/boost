package common

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc/auth"
	apitypes "github.com/filecoin-project/lotus/api/types"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/build"

	"github.com/filecoin-project/lotus/journal/alerting"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
)

var session = uuid.New()

type CommonAPI struct {
	fx.In

	Alerting     *alerting.Alerting
	APISecret    *lotus_dtypes.APIAlg
	ShutdownChan lotus_dtypes.ShutdownChan
}

type jwtPayload struct {
	Allow []auth.Permission
}

func (a *CommonAPI) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	var payload jwtPayload
	if _, err := jwt.Verify([]byte(token), (*jwt.HMACSHA)(a.APISecret), &payload); err != nil {
		return nil, fmt.Errorf("JWT Verification failed: %w", err)
	}

	return payload.Allow, nil
}

func (a *CommonAPI) AuthNew(ctx context.Context, perms []auth.Permission) ([]byte, error) {
	p := jwtPayload{
		Allow: perms, // TODO: consider checking validity
	}

	return jwt.Sign(&p, (*jwt.HMACSHA)(a.APISecret))
}

func (a *CommonAPI) Version(context.Context) (api.APIVersion, error) {
	return api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: api.BoostAPIVersion0,
	}, nil
}

func (a *CommonAPI) LogList(context.Context) ([]string, error) {
	return logging.GetSubsystems(), nil
}

func (a *CommonAPI) LogSetLevel(ctx context.Context, subsystem, level string) error {
	return logging.SetLogLevel(subsystem, level)
}

func (a *CommonAPI) LogAlerts(ctx context.Context) ([]alerting.Alert, error) {
	return a.Alerting.GetAlerts(), nil
}

func (a *CommonAPI) Shutdown(ctx context.Context) error {
	a.ShutdownChan <- struct{}{}
	return nil
}

func (a *CommonAPI) Session(ctx context.Context) (uuid.UUID, error) {
	return session, nil
}

func (a *CommonAPI) Closing(ctx context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil // relies on jsonrpc closing
}

func (a *CommonAPI) Discover(ctx context.Context) (apitypes.OpenRPCDocument, error) {
	return build.OpenRPCDiscoverJSON_Boost(), nil
}
