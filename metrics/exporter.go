package metrics

import (
	"net/http"
	_ "net/http/pprof"

	"contrib.go.opencensus.io/exporter/prometheus"
	logging "github.com/ipfs/go-log/v2"
	promclient "github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/stats/view"
)

var log = logging.Logger("metrics")

func Exporter(namespace string) http.Handler {
	if err := view.Register(DefaultViews...); err != nil {
		log.Errorf("cannot register default metric views: %s", err)
	}

	// Prometheus globals are exposed as interfaces, but the prometheus
	// OpenCensus exporter expects a concrete *Registry. The concrete type of
	// the globals are actually *Registry, so we downcast them, staying
	// defensive in case things change under the hood.
	registry, ok := promclient.DefaultRegisterer.(*promclient.Registry)
	if !ok {
		log.Warnf("failed to export default prometheus registry; some metrics will be unavailable; unexpected type: %T", promclient.DefaultRegisterer)
	}
	exporter, err := prometheus.NewExporter(prometheus.Options{
		Registry:  registry,
		Namespace: namespace,
	})
	if err != nil {
		log.Errorf("could not create the prometheus stats exporter: %v", err)
	}

	return exporter
}
