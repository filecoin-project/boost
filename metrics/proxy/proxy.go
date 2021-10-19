package proxy

import (
	"context"
	"reflect"

	"go.opencensus.io/tag"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/metrics"
)

func MetricedBoostAPI(a api.Boost) api.Boost {
	var out api.BoostStruct
	proxy(a, &out)
	return &out
}

func proxy(in interface{}, outstr interface{}) {
	outs := api.GetInternalStructs(outstr)
	for _, out := range outs {
		rint := reflect.ValueOf(out).Elem()
		ra := reflect.ValueOf(in)

		for f := 0; f < rint.NumField(); f++ {
			field := rint.Type().Field(f)
			fn := ra.MethodByName(field.Name)

			rint.Field(f).Set(reflect.MakeFunc(field.Type, func(args []reflect.Value) (results []reflect.Value) {
				ctx := args[0].Interface().(context.Context)
				// upsert function name into context
				ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, field.Name))
				stop := metrics.Timer(ctx, metrics.APIRequestDuration)
				defer stop()
				// pass tagged ctx back into function call
				args[0] = reflect.ValueOf(ctx)
				return fn.Call(args)
			}))
		}
	}
}
