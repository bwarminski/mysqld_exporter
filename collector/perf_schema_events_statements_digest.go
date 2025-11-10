// ABOUTME: Custom collector that extends perf_schema events_statements with derived ps-digest metrics.
// ABOUTME: Computes real-time percentages and rates that HubSpot's ps-collect-digest tooling provided historically.

// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `performance_schema.events_statements_summary_by_digest` with derived metrics.

package collector

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alecthomas/kingpin/v2"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const perfEventsStatementsDigestQuery = `
	SELECT
	    ifnull(SCHEMA_NAME, 'NONE') as SCHEMA_NAME,
	    DIGEST,
	    LEFT(DIGEST_TEXT, %d) as DIGEST_TEXT,
	    COUNT_STAR,
	    SUM_TIMER_WAIT,
	    SUM_ERRORS,
	    SUM_WARNINGS,
	    SUM_ROWS_AFFECTED,
	    SUM_ROWS_SENT,
	    SUM_ROWS_EXAMINED,
	    SUM_CREATED_TMP_DISK_TABLES,
	    SUM_CREATED_TMP_TABLES,
	    SUM_SORT_MERGE_PASSES,
	    SUM_SORT_ROWS,
	    SUM_NO_INDEX_USED
	  FROM performance_schema.events_statements_summary_by_digest
	  WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
	    AND LAST_SEEN > DATE_SUB(NOW(), INTERVAL %d SECOND)
	  ORDER BY SUM_TIMER_WAIT DESC
	  LIMIT %d
	`

const perfEventsStatementsDigestQueryMySQL = `
	SELECT
	    ifnull(SCHEMA_NAME, 'NONE') as SCHEMA_NAME,
	    DIGEST,
	    LEFT(DIGEST_TEXT, %d) as DIGEST_TEXT,
	    COUNT_STAR,
	    SUM_TIMER_WAIT,
	    SUM_LOCK_TIME,
	    SUM_CPU_TIME,
	    SUM_ERRORS,
	    SUM_WARNINGS,
	    SUM_ROWS_AFFECTED,
	    SUM_ROWS_SENT,
	    SUM_ROWS_EXAMINED,
	    SUM_CREATED_TMP_DISK_TABLES,
	    SUM_CREATED_TMP_TABLES,
	    SUM_SORT_MERGE_PASSES,
	    SUM_SORT_ROWS,
	    SUM_NO_INDEX_USED,
	    QUANTILE_95,
	    QUANTILE_99,
	    QUANTILE_999
	  FROM performance_schema.events_statements_summary_by_digest
	  WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
	    AND LAST_SEEN > DATE_SUB(NOW(), INTERVAL %d SECOND)
	  ORDER BY SUM_TIMER_WAIT DESC
	  LIMIT %d
	`

// Global totals query for calculating percentages
const perfEventsStatementsDigestTotalsQuery = `
	SELECT
	    IFNULL(SUM(COUNT_STAR), 0) as TOTAL_COUNT_STAR,
	    IFNULL(SUM(SUM_TIMER_WAIT), 0) as TOTAL_SUM_TIMER_WAIT,
	    IFNULL(SUM(SUM_ROWS_AFFECTED), 0) as TOTAL_ROWS_AFFECTED,
	    IFNULL(SUM(SUM_ROWS_SENT), 0) as TOTAL_ROWS_SENT,
	    IFNULL(SUM(SUM_ROWS_EXAMINED), 0) as TOTAL_ROWS_EXAMINED
	  FROM performance_schema.events_statements_summary_by_digest
	  WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
	    AND LAST_SEEN > DATE_SUB(NOW(), INTERVAL %d SECOND)
	`

// Tunable flags.
var (
	perfEventsStatementsDigestCustomLimit = kingpin.Flag(
		"collect.perf_schema.eventsstatements.digest.limit",
		"Limit the number of events statements digests by response time for digest collector",
	).Default("250").Int()
	perfEventsStatementsDigestCustomTimeLimit = kingpin.Flag(
		"collect.perf_schema.eventsstatements.digest.timelimit",
		"Limit how old the 'last_seen' events statements can be, in seconds for digest collector",
	).Default("86400").Int()
	perfEventsStatementsDigestCustomTextLimit = kingpin.Flag(
		"collect.perf_schema.eventsstatements.digest.digest_text_limit",
		"Maximum length of the normalized statement text for digest collector",
	).Default("120").Int()
)

// Metric descriptors for derived ps-digest metrics.
var (
	performanceSchemaEventsStatementsDigestCountPctDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_count_pct"),
		"The percentage of total query count by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsDigestTimerPctDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_timer_pct"),
		"The percentage of total query time by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsDigestAvgTimerMsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_avg_timer_ms"),
		"The average query time in milliseconds by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsDigestRowsAffectedPctDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_rows_affected_pct"),
		"The percentage of total rows affected by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsDigestRowsSentPctDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_rows_sent_pct"),
		"The percentage of total rows sent by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsDigestRowsExaminedPctDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_rows_examined_pct"),
		"The percentage of total rows examined by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsDigestAvgRowsAffectedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_avg_rows_affected"),
		"The average rows affected per query by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsDigestAvgRowsSentDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_avg_rows_sent"),
		"The average rows sent per query by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsDigestAvgRowsExaminedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_digest_avg_rows_examined"),
		"The average rows examined per query by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
)

// ScrapePerfEventsStatementsDigest collects from `performance_schema.events_statements_summary_by_digest` with derived metrics.
type ScrapePerfEventsStatementsDigest struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfEventsStatementsDigest) Name() string {
	return "perf_schema.eventsstatements.digest"
}

// Help describes the role of the Scraper.
func (ScrapePerfEventsStatementsDigest) Help() string {
	return "Collect derived ps-digest metrics from performance_schema.events_statements_summary_by_digest"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfEventsStatementsDigest) Version() float64 {
	return 5.6
}

// DigestTotals holds aggregate totals for percentage calculations
type DigestTotals struct {
	TotalCountStar    uint64
	TotalSumTimerWait uint64
	TotalRowsAffected uint64
	TotalRowsSent     uint64
	TotalRowsExamined uint64
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfEventsStatementsDigest) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	mysqlVersion8028 := instance.flavor == FlavorMySQL && instance.version.GTE(semver.MustParse("8.0.28"))

	db := instance.getDB()

	// First, get the totals for percentage calculations
	totalsQuery := fmt.Sprintf(perfEventsStatementsDigestTotalsQuery, *perfEventsStatementsDigestCustomTimeLimit)
	totalsRows, err := db.QueryContext(ctx, totalsQuery)
	if err != nil {
		return err
	}
	defer totalsRows.Close()

	var totals DigestTotals
	if totalsRows.Next() {
		err := totalsRows.Scan(
			&totals.TotalCountStar,
			&totals.TotalSumTimerWait,
			&totals.TotalRowsAffected,
			&totals.TotalRowsSent,
			&totals.TotalRowsExamined,
		)
		if err != nil {
			return err
		}
	}

	// Now get the digest data
	perfQuery := perfEventsStatementsDigestQuery
	if mysqlVersion8028 {
		perfQuery = perfEventsStatementsDigestQueryMySQL
	}

	perfQuery = fmt.Sprintf(
		perfQuery,
		*perfEventsStatementsDigestCustomTextLimit,
		*perfEventsStatementsDigestCustomTimeLimit,
		*perfEventsStatementsDigestCustomLimit,
	)

	perfSchemaEventsStatementsRows, err := db.QueryContext(ctx, perfQuery)
	if err != nil {
		return err
	}
	defer perfSchemaEventsStatementsRows.Close()

	var (
		schemaName, digest, digestText       string
		count, queryTime, lockTime, cpuTime  uint64
		errors, warnings                     uint64
		rowsAffected, rowsSent, rowsExamined uint64
		tmpTables, tmpDiskTables             uint64
		sortMergePasses, sortRows            uint64
		noIndexUsed                          uint64
		quantile95, quantile99, quantile999  uint64
	)

	for perfSchemaEventsStatementsRows.Next() {
		var err error
		if mysqlVersion8028 {
			err = perfSchemaEventsStatementsRows.Scan(
				&schemaName, &digest, &digestText, &count, &queryTime, &lockTime, &cpuTime, &errors, &warnings, &rowsAffected, &rowsSent, &rowsExamined, &tmpDiskTables, &tmpTables, &sortMergePasses, &sortRows, &noIndexUsed, &quantile95, &quantile99, &quantile999,
			)
		} else {
			err = perfSchemaEventsStatementsRows.Scan(
				&schemaName, &digest, &digestText, &count, &queryTime, &errors, &warnings, &rowsAffected, &rowsSent, &rowsExamined, &tmpDiskTables, &tmpTables, &sortMergePasses, &sortRows, &noIndexUsed,
			)
		}
		if err != nil {
			return err
		}

		// Calculate derived metrics based on HubSpot's ps-collect-digest formulas
		labels := []string{schemaName, digest, digestText}

		// PCT_COUNT_STAR: COUNT_STAR / @sum_count_star
		if totals.TotalCountStar > 0 {
			pctCountStar := float64(count) / float64(totals.TotalCountStar)
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestCountPctDesc, prometheus.GaugeValue, pctCountStar,
				labels...,
			)
		}

		// PCT_TIMER_WAIT: SUM_TIMER_WAIT / @sum_sum_timer_wait
		if totals.TotalSumTimerWait > 0 {
			pctTimerWait := float64(queryTime) / float64(totals.TotalSumTimerWait)
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestTimerPctDesc, prometheus.GaugeValue, pctTimerWait,
				labels...,
			)
		}

		// AVG_TIMER_WAIT_MS: SUM_TIMER_WAIT / COUNT_STAR / 1000 / 1000 / 1000
		if count > 0 {
			avgTimerWaitMs := float64(queryTime) / float64(count) / picoSeconds * 1000
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestAvgTimerMsDesc, prometheus.GaugeValue, avgTimerWaitMs,
				labels...,
			)
		}

		// PCT_ROWS_AFFECTED: SUM_ROWS_AFFECTED / @sum_sum_rows_affected
		if totals.TotalRowsAffected > 0 {
			pctRowsAffected := float64(rowsAffected) / float64(totals.TotalRowsAffected)
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestRowsAffectedPctDesc, prometheus.GaugeValue, pctRowsAffected,
				labels...,
			)
		}

		// AVG_ROWS_AFFECTED: SUM_ROWS_AFFECTED / COUNT_STAR
		if count > 0 {
			avgRowsAffected := float64(rowsAffected) / float64(count)
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestAvgRowsAffectedDesc, prometheus.GaugeValue, avgRowsAffected,
				labels...,
			)
		}

		// PCT_ROWS_SENT: SUM_ROWS_SENT / @sum_sum_rows_sent
		if totals.TotalRowsSent > 0 {
			pctRowsSent := float64(rowsSent) / float64(totals.TotalRowsSent)
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestRowsSentPctDesc, prometheus.GaugeValue, pctRowsSent,
				labels...,
			)
		}

		// AVG_ROWS_SENT: SUM_ROWS_SENT / COUNT_STAR
		if count > 0 {
			avgRowsSent := float64(rowsSent) / float64(count)
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestAvgRowsSentDesc, prometheus.GaugeValue, avgRowsSent,
				labels...,
			)
		}

		// PCT_ROWS_EXAMINED: SUM_ROWS_EXAMINED / @sum_sum_rows_examined
		if totals.TotalRowsExamined > 0 {
			pctRowsExamined := float64(rowsExamined) / float64(totals.TotalRowsExamined)
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestRowsExaminedPctDesc, prometheus.GaugeValue, pctRowsExamined,
				labels...,
			)
		}

		// AVG_ROWS_EXAMINED: SUM_ROWS_EXAMINED / COUNT_STAR
		if count > 0 {
			avgRowsExamined := float64(rowsExamined) / float64(count)
			ch <- prometheus.MustNewConstMetric(
				performanceSchemaEventsStatementsDigestAvgRowsExaminedDesc, prometheus.GaugeValue, avgRowsExamined,
				labels...,
			)
		}
	}
	return nil
}

// check interface
var _ Scraper = ScrapePerfEventsStatementsDigest{}
