package repo

import (
	"github.com/filecoin-project/boost/node/config"
)

var Boost boost

type boost struct{}

func (f boost) Type() string {
	return "Boost"
}

func (f boost) Config() interface{} {
	return config.DefaultBoost()
}

func (boost) SupportsStagingDeals() {}

func (boost) APIFlags() []string {
	return []string{"boost-api-url"}
}

func (boost) RepoFlags() []string {
	return []string{"boost-repo"}
}

func (boost) APIInfoEnvVars() (primary string, fallbacks []string, deprecated []string) {
	return "BOOST_API_INFO", nil, nil
}
