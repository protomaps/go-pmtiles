package pmtiles

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var buildInfoMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "pmtiles",
	Name:      "buildinfo",
}, []string{"version", "revision"})

var buildTimeMetric = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "pmtiles",
	Name:      "buildtime",
})

func init() {
	err := prometheus.Register(buildInfoMetric)
	if err != nil {
		fmt.Println("Error registering metric", err)
	}
	err = prometheus.Register(buildTimeMetric)
	if err != nil {
		fmt.Println("Error registering metric", err)
	}
}

// SetBuildInfo initializes static metrics with pmtiles version, git hash, and build time
func SetBuildInfo(version, commit, date string) {
	buildInfoMetric.WithLabelValues(version, commit).Set(1)
	time, err := time.Parse(time.RFC3339, date)
	if err == nil {
		buildTimeMetric.Set(float64(time.Unix()))
	} else {
		buildTimeMetric.Set(0)
	}
}

type metrics struct {
	// overall requests: # requests, request duration, response size by archive/status code
	requests        *prometheus.CounterVec
	responseSize    *prometheus.HistogramVec
	requestDuration *prometheus.HistogramVec
	// dir cache: # requests, hits, cache entries, cache bytes, cache bytes limit
	dirCacheEntries    prometheus.Gauge
	dirCacheSizeBytes  prometheus.Gauge
	dirCacheLimitBytes prometheus.Gauge
	dirCacheRequests   *prometheus.CounterVec
	// requests to bucket: # total, response duration by archive/status code
	bucketRequests        *prometheus.CounterVec
	bucketRequestDuration *prometheus.HistogramVec
	// misc
	reloads *prometheus.CounterVec
}

// utility to time an overall tile request
type requestTracker struct {
	finished bool
	start    time.Time
	metrics  *metrics
}

func (m *metrics) startRequest() *requestTracker {
	return &requestTracker{start: time.Now(), metrics: m}
}

func (r *requestTracker) finish(ctx context.Context, archive, handler string, status, responseSize int, logDetails bool) {
	if !r.finished {
		r.finished = true
		// exclude archive path from "not found" metrics to limit cardinality on requests for nonexistant archives
		statusString := strconv.Itoa(status)
		if status == 404 {
			archive = ""
		} else if isCanceled(ctx) {
			statusString = "canceled"
		}

		labels := []string{archive, handler, statusString}
		r.metrics.requests.WithLabelValues(labels...).Inc()
		if logDetails {
			r.metrics.responseSize.WithLabelValues(labels...).Observe(float64(responseSize))
			r.metrics.requestDuration.WithLabelValues(labels...).Observe(time.Since(r.start).Seconds())
		}
	}
}

// utility to time an individual request to the underlying bucket
type bucketRequestTracker struct {
	finished bool
	start    time.Time
	metrics  *metrics
	archive  string
	kind     string
}

func (m *metrics) startBucketRequest(archive, kind string) *bucketRequestTracker {
	return &bucketRequestTracker{start: time.Now(), metrics: m, archive: archive, kind: kind}
}

func (r *bucketRequestTracker) finish(ctx context.Context, status string) {
	if !r.finished {
		r.finished = true
		// exclude archive path from "not found" metrics to limit cardinality on requests for nonexistant archives
		if status == "404" || status == "403" {
			r.archive = ""
		} else if isCanceled(ctx) {
			status = "canceled"
		}
		r.metrics.bucketRequests.WithLabelValues(r.archive, r.kind, status).Inc()
		r.metrics.bucketRequestDuration.WithLabelValues(r.archive, status).Observe(time.Since(r.start).Seconds())
	}
}

// misc helpers

func (m *metrics) reloadFile(name string) {
	m.reloads.WithLabelValues(name).Inc()
}

func (m *metrics) initCacheStats(limitBytes int) {
	m.dirCacheLimitBytes.Set(float64(limitBytes))
	m.updateCacheStats(0, 0)
}

func (m *metrics) updateCacheStats(sizeBytes, entries int) {
	m.dirCacheEntries.Set(float64(entries))
	m.dirCacheSizeBytes.Set(float64(sizeBytes))
}

func (m *metrics) cacheRequest(archive, kind, status string) {
	m.dirCacheRequests.WithLabelValues(archive, kind, status).Inc()
}

func register[K prometheus.Collector](logger *log.Logger, metric K) K {
	if err := prometheus.Register(metric); err != nil {
		logger.Println(err)
	}
	return metric
}

func createMetrics(scope string, logger *log.Logger) *metrics {
	namespace := "pmtiles"
	durationBuckets := prometheus.DefBuckets
	kib := 1024.0
	mib := kib * kib
	sizeBuckets := []float64{1.0 * kib, 5.0 * kib, 10.0 * kib, 25.0 * kib, 50.0 * kib, 100 * kib, 250 * kib, 500 * kib, 1.0 * mib}

	return &metrics{
		// overall requests
		requests: register(logger, prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "requests_total",
			Help:      "Overall number of requests to the service",
		}, []string{"archive", "handler", "status"})),
		responseSize: register(logger, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "response_size_bytes",
			Help:      "Overall response size in bytes",
			Buckets:   sizeBuckets,
		}, []string{"archive", "handler", "status"})),
		requestDuration: register(logger, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "request_duration_seconds",
			Help:      "Overall request duration in seconds",
			Buckets:   durationBuckets,
		}, []string{"archive", "handler", "status"})),

		// dir cache
		dirCacheEntries: register(logger, prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "dir_cache_entries",
			Help:      "Number of directories in the cache",
		})),
		dirCacheSizeBytes: register(logger, prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "dir_cache_size_bytes",
			Help:      "Current directory cache usage in bytes",
		})),
		dirCacheLimitBytes: register(logger, prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "dir_cache_limit_bytes",
			Help:      "Maximum directory cache size limit in bytes",
		})),
		dirCacheRequests: register(logger, prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "dir_cache_requests",
			Help:      "Requests to the directory cache by archive and status (hit/miss)",
		}, []string{"archive", "kind", "status"})),

		// requests to bucket
		bucketRequests: register(logger, prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "bucket_requests_total",
			Help:      "Requests to the underlying bucket",
		}, []string{"archive", "kind", "status"})),
		bucketRequestDuration: register(logger, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "bucket_request_duration_seconds",
			Help:      "Request duration in seconds for individual requests to the underlying bucket",
			Buckets:   durationBuckets,
		}, []string{"archive", "status"})),

		// misc
		reloads: register(logger, prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: scope,
			Name:      "bucket_reloads",
			Help:      "Number of times an archive was reloaded due to the etag changing",
		}, []string{"archive"})),
	}
}
