// ABOUTME: Backward compatibility stub for the old perf_schema.eventsstatements.digest collector.
// ABOUTME: This collector enables digest metrics within the main events_statements collector instead of running separately.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// ScrapePerfEventsStatementsDigest is a backward compatibility stub that enables digest metrics
// within the main ScrapePerfEventsStatements collector instead of running as a separate collector.
type ScrapePerfEventsStatementsDigest struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfEventsStatementsDigest) Name() string {
	return "perf_schema.eventsstatements.digest"
}

// Help describes the role of the Scraper.
func (ScrapePerfEventsStatementsDigest) Help() string {
	return "DEPRECATED: Use --collect.perf_schema.eventsstatements.digest_metrics instead. This enables digest metrics in the main events_statements collector."
}

// Version of MySQL from which scraper is available.
func (ScrapePerfEventsStatementsDigest) Version() float64 {
	return 5.6
}

// Scrape does nothing but enables digest metrics in the main collector by setting the flag.
func (ScrapePerfEventsStatementsDigest) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	// Set the digest metrics flag to true when this collector is enabled
	*perfEventsStatementsDigestMetrics = true

	// Log deprecation warning
	logger.Warn("DEPRECATED: perf_schema.eventsstatements.digest collector is deprecated. Use --collect.perf_schema.eventsstatements.digest_metrics flag instead.")

	// Don't emit any metrics - they will be handled by the main events_statements collector
	return nil
}

// check interface
var _ Scraper = ScrapePerfEventsStatementsDigest{}