package gql

import (
	"time"

	gqltypes "github.com/filecoin-project/boost/gql/types"
)

func bigIntToTime(i *gqltypes.BigInt) *time.Time {
	if i == nil {
		return nil
	}
	val := (*i).Int64()
	asTime := time.Unix(val/1000, (val%1000)*1e6)
	return &asTime
}
