package metrics

import (
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Pipeline metrics — atomic counters safe for concurrent goroutine access.
var (
	EventsReceived  atomic.Int64
	EventsProcessed atomic.Int64
	AlertsTriggered atomic.Int64

	// Sum of processing durations in microseconds and count, used to compute average.
	processingMicrosSum   atomic.Int64
	processingMicrosCount atomic.Int64

	// Set at startup; not modified after.
	PipelineMode string
	WorkerCount  int
)

// RecordProcessingDuration records the elapsed time since start in the processing metrics.
func RecordProcessingDuration(start time.Time) {
	microseconds := time.Since(start).Microseconds()
	processingMicrosSum.Add(microseconds)
	processingMicrosCount.Add(1)
}

// Serve starts an HTTP server on the given address exposing /metrics in
// Prometheus text exposition format. Runs in its own goroutine.
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", handler)

	go func() {
		slog.Info("Metrics server starting", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("Metrics server failed", "error", err)
		}
	}()
}

func handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	received := EventsReceived.Load()
	processed := EventsProcessed.Load()
	triggered := AlertsTriggered.Load()
	durSum := processingMicrosSum.Load()
	durCount := processingMicrosCount.Load()

	avgMicroseconds := float64(0)
	if durCount > 0 {
		avgMicroseconds = float64(durSum) / float64(durCount)
	}

	fmt.Fprintf(w, "# HELP events_received_total Total sensor.updated events consumed from RabbitMQ.\n")
	fmt.Fprintf(w, "# TYPE events_received_total counter\n")
	fmt.Fprintf(w, "events_received_total %d\n\n", received)

	fmt.Fprintf(w, "# HELP events_processed_total Total events that completed evaluation.\n")
	fmt.Fprintf(w, "# TYPE events_processed_total counter\n")
	fmt.Fprintf(w, "events_processed_total %d\n\n", processed)

	fmt.Fprintf(w, "# HELP alerts_triggered_total Total alerts triggered by threshold crossings.\n")
	fmt.Fprintf(w, "# TYPE alerts_triggered_total counter\n")
	fmt.Fprintf(w, "alerts_triggered_total %d\n\n", triggered)

	fmt.Fprintf(w, "# HELP event_processing_avg_microseconds Average event processing duration in microseconds.\n")
	fmt.Fprintf(w, "# TYPE event_processing_avg_microseconds gauge\n")
	fmt.Fprintf(w, "event_processing_avg_microseconds %.1f\n\n", avgMicroseconds)

	fmt.Fprintf(w, "# HELP pipeline_info Pipeline configuration.\n")
	fmt.Fprintf(w, "# TYPE pipeline_info gauge\n")
	fmt.Fprintf(w, "pipeline_info{mode=%q,worker_count=\"%d\"} 1\n", PipelineMode, WorkerCount)
}
