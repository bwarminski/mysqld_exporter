// Copyright 2018 The Prometheus Authors
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

// Scrape `performance_schema.events_statements_summary_by_digest`.

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const perfEventsStatementsQuery = `
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
	  FROM (
	    SELECT *
	    FROM performance_schema.events_statements_summary_by_digest
	    WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
	      AND LAST_SEEN > DATE_SUB(NOW(), INTERVAL %d SECOND)
	    ORDER BY LAST_SEEN DESC
	  )Q
	  GROUP BY
	    Q.SCHEMA_NAME,
	    Q.DIGEST,
	    Q.DIGEST_TEXT,
	    Q.COUNT_STAR,
	    Q.SUM_TIMER_WAIT,
	    Q.SUM_ERRORS,
	    Q.SUM_WARNINGS,
	    Q.SUM_ROWS_AFFECTED,
	    Q.SUM_ROWS_SENT,
	    Q.SUM_ROWS_EXAMINED,
	    Q.SUM_CREATED_TMP_DISK_TABLES,
	    Q.SUM_CREATED_TMP_TABLES,
	    Q.SUM_SORT_MERGE_PASSES,
	    Q.SUM_SORT_ROWS,
	    Q.SUM_NO_INDEX_USED
	  ORDER BY SUM_TIMER_WAIT DESC
	  LIMIT %d
	`

const perfEventsStatementsQueryMySQL = `
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
	  FROM (
	    SELECT *
	    FROM performance_schema.events_statements_summary_by_digest
	    WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
	      AND LAST_SEEN > DATE_SUB(NOW(), INTERVAL %d SECOND)
	    ORDER BY LAST_SEEN DESC
	  )Q
	  GROUP BY
	    Q.SCHEMA_NAME,
	    Q.DIGEST,
	    Q.DIGEST_TEXT,
	    Q.COUNT_STAR,
	    Q.SUM_TIMER_WAIT,
	    Q.SUM_LOCK_TIME,
	    Q.SUM_CPU_TIME,
	    Q.SUM_ERRORS,
	    Q.SUM_WARNINGS,
	    Q.SUM_ROWS_AFFECTED,
	    Q.SUM_ROWS_SENT,
	    Q.SUM_ROWS_EXAMINED,
	    Q.SUM_CREATED_TMP_DISK_TABLES,
	    Q.SUM_CREATED_TMP_TABLES,
	    Q.SUM_SORT_MERGE_PASSES,
	    Q.SUM_SORT_ROWS,
	    Q.SUM_NO_INDEX_USED,
	    Q.QUANTILE_95,
	    Q.QUANTILE_99,
	    Q.QUANTILE_999
	  ORDER BY SUM_TIMER_WAIT DESC
	  LIMIT %d
	`

// Global totals query for calculating percentages
const perfEventsStatementsQueryTotals = `
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
	perfEventsStatementsLimit = kingpin.Flag(
		"collect.perf_schema.eventsstatements.limit",
		"Limit the number of events statements digests by response time",
	).Default("250").Int()
	perfEventsStatementsTimeLimit = kingpin.Flag(
		"collect.perf_schema.eventsstatements.timelimit",
		"Limit how old the 'last_seen' events statements can be, in seconds",
	).Default("86400").Int()
	perfEventsStatementsDigestTextLimit = kingpin.Flag(
		"collect.perf_schema.eventsstatements.digest_text_limit",
		"Maximum length of the normalized statement text",
	).Default("120").Int()
	perfEventsStatementsDigestMetrics = kingpin.Flag(
		"collect.perf_schema.eventsstatements.digest_metrics",
		"Enable derived digest percentage and average metrics",
	).Default("false").Bool()
)

// Metric descriptors.
var (
	performanceSchemaEventsStatementsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_total"),
		"The total count of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_seconds_total"),
		"The total time of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsLockTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_lock_time_seconds_total"),
		"The total lock time of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsCpuTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_cpu_time_seconds_total"),
		"The total cpu time of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsErrorsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_errors_total"),
		"The errors of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsWarningsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_warnings_total"),
		"The warnings of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsRowsAffectedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_rows_affected_total"),
		"The total rows affected of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsRowsSentDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_rows_sent_total"),
		"The total rows sent of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsRowsExaminedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_rows_examined_total"),
		"The total rows examined of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsTmpTablesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_tmp_tables_total"),
		"The total tmp tables of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsTmpDiskTablesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_tmp_disk_tables_total"),
		"The total tmp disk tables of events statements by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsSortMergePassesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_sort_merge_passes_total"),
		"The total number of merge passes by the sort algorithm performed by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsSortRowsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_sort_rows_total"),
		"The total number of sorted rows by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsNoIndexUsedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_no_index_used_total"),
		"The total number of statements that used full table scans by digest.",
		[]string{"schema", "digest", "digest_text"}, nil,
	)
	performanceSchemaEventsStatementsLatency = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "events_statements_latency"),
		"A summary of statement latency by digest",
		[]string{"schema", "digest", "digest_text"}, nil,
	)

	// Digest-derived metric descriptors (enabled via --collect.perf_schema.eventsstatements.digest_metrics)
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

// ScrapePerfEventsStatements collects from `performance_schema.events_statements_summary_by_digest`.
type ScrapePerfEventsStatements struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfEventsStatements) Name() string {
	return "perf_schema.eventsstatements"
}

// Help describes the role of the Scraper.
func (ScrapePerfEventsStatements) Help() string {
	return "Collect metrics from performance_schema.events_statements_summary_by_digest"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfEventsStatements) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfEventsStatements) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	mysqlVersion8028 := instance.flavor == FlavorMySQL && instance.version.GTE(semver.MustParse("8.0.28"))

	perfQuery := perfEventsStatementsQuery
	if mysqlVersion8028 {
		perfQuery = perfEventsStatementsQueryMySQL
	}

	perfQuery = fmt.Sprintf(
		perfQuery,
		*perfEventsStatementsDigestTextLimit,
		*perfEventsStatementsTimeLimit,
		*perfEventsStatementsLimit,
	)

	db := instance.getDB()
	// Timers here are returned in picoseconds.
	logger.Debug("perf events statements query start", "query", "digest_select", "sql", perfQuery)
	perfSelectStart := time.Now()
	perfSchemaEventsStatementsRows, err := db.QueryContext(ctx, perfQuery)
	if err != nil {
		return err
	}
	defer perfSchemaEventsStatementsRows.Close()
	logger.Debug("perf events statements query done", "query", "digest_select", "duration", time.Since(perfSelectStart))

	// Collect all data for digest calculations if enabled
	type digestRow struct {
		schemaName, digest, digestText       string
		count, queryTime, lockTime, cpuTime  uint64
		errors, warnings                     uint64
		rowsAffected, rowsSent, rowsExamined uint64
		tmpTables, tmpDiskTables             uint64
		sortMergePasses, sortRows            uint64
		noIndexUsed                          uint64
		quantile95, quantile99, quantile999  uint64
	}

	var allRows []digestRow

	// Get totals from the full table (not limited) for accurate percentage calculations
	var totalCount, totalTimerWait, totalRowsAffected, totalRowsSent, totalRowsExamined uint64
	if *perfEventsStatementsDigestMetrics {
		totalsQuery := fmt.Sprintf(perfEventsStatementsQueryTotals, *perfEventsStatementsTimeLimit)
		logger.Debug("perf events statements query start", "query", "digest_totals", "sql", totalsQuery)
		totalsStart := time.Now()
		totalsRows, err := db.QueryContext(ctx, totalsQuery)
		if err != nil {
			return err
		}
		defer totalsRows.Close()

		if totalsRows.Next() {
			err := totalsRows.Scan(&totalCount, &totalTimerWait, &totalRowsAffected, &totalRowsSent, &totalRowsExamined)
			if err != nil {
				return err
			}
		}
		logger.Debug("perf events statements query done", "query", "digest_totals", "duration", time.Since(totalsStart))
	}

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

	rowCount := 0
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

		// Store row data for potential digest calculations
		row := digestRow{
			schemaName, digest, digestText,
			count, queryTime, lockTime, cpuTime,
			errors, warnings,
			rowsAffected, rowsSent, rowsExamined,
			tmpTables, tmpDiskTables,
			sortMergePasses, sortRows,
			noIndexUsed,
			quantile95, quantile99, quantile999,
		}
		allRows = append(allRows, row)
		rowCount++
	}
	logger.Debug("perf events statements rows read", "rows", rowCount, "duration", time.Since(perfSelectStart))

	// Now emit all metrics for each row
	emitStart := time.Now()
	for _, row := range allRows {
		labels := []string{row.schemaName, row.digest, row.digestText}

		// Emit existing performance_schema metrics
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsDesc, prometheus.CounterValue, float64(row.count),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsTimeDesc, prometheus.CounterValue, float64(row.queryTime)/picoSeconds,
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsLockTimeDesc, prometheus.CounterValue, float64(row.lockTime)/picoSeconds,
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsCpuTimeDesc, prometheus.CounterValue, float64(row.cpuTime)/picoSeconds,
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsErrorsDesc, prometheus.CounterValue, float64(row.errors),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsWarningsDesc, prometheus.CounterValue, float64(row.warnings),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsRowsAffectedDesc, prometheus.CounterValue, float64(row.rowsAffected),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsRowsSentDesc, prometheus.CounterValue, float64(row.rowsSent),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsRowsExaminedDesc, prometheus.CounterValue, float64(row.rowsExamined),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsTmpTablesDesc, prometheus.CounterValue, float64(row.tmpTables),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsTmpDiskTablesDesc, prometheus.CounterValue, float64(row.tmpDiskTables),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsSortMergePassesDesc, prometheus.CounterValue, float64(row.sortMergePasses),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsSortRowsDesc, prometheus.CounterValue, float64(row.sortRows),
			labels...,
		)
		ch <- prometheus.MustNewConstMetric(
			performanceSchemaEventsStatementsNoIndexUsedDesc, prometheus.CounterValue, float64(row.noIndexUsed),
			labels...,
		)
		ch <- prometheus.MustNewConstSummary(performanceSchemaEventsStatementsLatency, row.count, float64(row.queryTime)/picoSeconds, map[float64]float64{
			95:  float64(row.quantile95) / picoSeconds,
			99:  float64(row.quantile99) / picoSeconds,
			999: float64(row.quantile999) / picoSeconds,
		}, labels...)

		// Emit digest-derived metrics if enabled
		if *perfEventsStatementsDigestMetrics {
			// PCT_COUNT_STAR: COUNT_STAR / total_count_star
			if totalCount > 0 {
				pctCountStar := float64(row.count) / float64(totalCount)
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestCountPctDesc, prometheus.GaugeValue, pctCountStar,
					labels...,
				)
			}

			// PCT_TIMER_WAIT: SUM_TIMER_WAIT / total_timer_wait
			if totalTimerWait > 0 {
				pctTimerWait := float64(row.queryTime) / float64(totalTimerWait)
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestTimerPctDesc, prometheus.GaugeValue, pctTimerWait,
					labels...,
				)
			}

			// AVG_TIMER_WAIT_MS: SUM_TIMER_WAIT / COUNT_STAR / 1000 / 1000 / 1000
			if row.count > 0 {
				avgTimerWaitMs := float64(row.queryTime) / float64(row.count) / picoSeconds * 1000
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestAvgTimerMsDesc, prometheus.GaugeValue, avgTimerWaitMs,
					labels...,
				)
			}

			// PCT_ROWS_AFFECTED: SUM_ROWS_AFFECTED / total_rows_affected
			if totalRowsAffected > 0 {
				pctRowsAffected := float64(row.rowsAffected) / float64(totalRowsAffected)
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestRowsAffectedPctDesc, prometheus.GaugeValue, pctRowsAffected,
					labels...,
				)
			}

			// AVG_ROWS_AFFECTED: SUM_ROWS_AFFECTED / COUNT_STAR
			if row.count > 0 {
				avgRowsAffected := float64(row.rowsAffected) / float64(row.count)
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestAvgRowsAffectedDesc, prometheus.GaugeValue, avgRowsAffected,
					labels...,
				)
			}

			// PCT_ROWS_SENT: SUM_ROWS_SENT / total_rows_sent
			if totalRowsSent > 0 {
				pctRowsSent := float64(row.rowsSent) / float64(totalRowsSent)
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestRowsSentPctDesc, prometheus.GaugeValue, pctRowsSent,
					labels...,
				)
			}

			// AVG_ROWS_SENT: SUM_ROWS_SENT / COUNT_STAR
			if row.count > 0 {
				avgRowsSent := float64(row.rowsSent) / float64(row.count)
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestAvgRowsSentDesc, prometheus.GaugeValue, avgRowsSent,
					labels...,
				)
			}

			// PCT_ROWS_EXAMINED: SUM_ROWS_EXAMINED / total_rows_examined
			if totalRowsExamined > 0 {
				pctRowsExamined := float64(row.rowsExamined) / float64(totalRowsExamined)
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestRowsExaminedPctDesc, prometheus.GaugeValue, pctRowsExamined,
					labels...,
				)
			}

			// AVG_ROWS_EXAMINED: SUM_ROWS_EXAMINED / COUNT_STAR
			if row.count > 0 {
				avgRowsExamined := float64(row.rowsExamined) / float64(row.count)
				ch <- prometheus.MustNewConstMetric(
					performanceSchemaEventsStatementsDigestAvgRowsExaminedDesc, prometheus.GaugeValue, avgRowsExamined,
					labels...,
				)
			}
		}
	}
	logger.Debug("perf events statements metrics emitted", "rows", rowCount, "duration", time.Since(emitStart))

	return nil
}

// check interface
var _ Scraper = ScrapePerfEventsStatements{}
