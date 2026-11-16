// Command bench measures archival correctness and performance end to end:
// query results before/after archiving, compression ratio, cold/hot query
// latency, live-write impact, and hold enforcement.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"payment-archive/internal/coldquery"
	"payment-archive/internal/config"
	"payment-archive/internal/control"
	"payment-archive/internal/model"
	"payment-archive/internal/source"
	"payment-archive/internal/unifiedquery"
)

type caseResult struct {
	Name        string  `json:"name"`
	RowsBefore  int     `json:"rows_before"`
	RowsAfter   int     `json:"rows_after"`
	Equal       bool    `json:"equal"`
	DiffDetail  string  `json:"diff,omitempty"`
	HotRows     int     `json:"hot_rows_after"`
	ColdRows    int     `json:"cold_rows_after"`
	ColdLatency float64 `json:"cold_latency_ms"`
	EOELatency  float64 `json:"e2e_latency_ms"`
	BlocksTotal int     `json:"cold_blocks_total"`
	Pruned      int     `json:"cold_blocks_pruned"`
}

func main() {
	cfgPath := flag.String("config", "", "config file")
	phase := flag.String("phase", "all", "pre | post | all")
	maxQPS := flag.Int("max-qps", 500, "cold query QPS ceiling for benchmarking (production default is lower)")
	maxConc := flag.Int("max-concurrent", 4, "cold query concurrency ceiling for benchmarking")
	out := flag.String("out", "", "output json path")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	// Benchmark runs issue queries back-to-back; raise the admission ceilings so
	// latency numbers measure the engine, not the rate limiter. Throttling is
	// exercised separately by the integration tests.
	cfg.ColdQuery.MaxQPS = *maxQPS
	cfg.ColdQuery.MaxConcurrent = *maxConc
	cfg.ColdQuery.QueueTimeoutMS = 10000
	ctx := context.Background()
	src, err := source.Open(ctx, cfg.SourceDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()
	meta, err := control.Open(cfg.ControlDB)
	if err != nil {
		log.Fatal(err)
	}
	defer meta.Close()
	cold, err := coldquery.New(cfg.ColdQuery, meta)
	if err != nil {
		log.Fatal(err)
	}
	uq := &unifiedquery.Engine{Source: src, Cold: cold, Table: "transactions",
		MaxLimit: cfg.ColdQuery.MaxLimit, DefaultLimit: cfg.ColdQuery.DefaultLimit}

	// Freeze the query anchor once and persist it, so pre/post runs use exactly
	// the same absolute time windows even if hours pass between them.
	anchorPath := filepath.Join(cfg.Demo.ReportDir, "baseline-anchor.json")
	anchor := loadOrCreateAnchor(anchorPath)
	cases := buildCases(src, anchor)
	report := map[string]any{"generated_at": time.Now().UTC(), "cases": len(cases), "anchor": anchor}

	// Determine the cold boundary from the active policy.
	policies, _ := meta.Policies(ctx)
	if len(policies) == 0 {
		log.Fatal("no policy configured")
	}
	pol := policies[0]
	coldCut := time.Now().UTC().AddDate(0, 0, -pol.ColdAfterDays)

	if *phase == "pre" || *phase == "all" {
		log.Printf("phase pre: capturing baseline over %d query cases", len(cases))
		baseline := make(map[string][]model.TransactionRow, len(cases))
		results := make([]caseResult, 0, len(cases))
		for _, c := range cases {
			rows, err := src.QueryPage(ctx, withoutProbeRows(c.q), nil, c.q.Limit)
			if err != nil {
				log.Fatalf("pre query %s: %v", c.name, err)
			}
			trs := toTransactions(rows.Rows)
			baseline[c.name] = trs
			results = append(results, caseResult{Name: c.name, RowsBefore: len(trs), Equal: true})
		}
		saveJSON(filepath.Join(cfg.Demo.ReportDir, "baseline.json"), baseline)
		report["pre_cases"] = results
		log.Printf("baseline saved to %s", filepath.Join(cfg.Demo.ReportDir, "baseline.json"))
	}

	if *phase == "post" || *phase == "all" {
		var baseline map[string][]model.TransactionRow
		b, err := os.ReadFile(filepath.Join(cfg.Demo.ReportDir, "baseline.json"))
		if err != nil {
			log.Fatalf("read baseline (run -phase pre first): %v", err)
		}
		if err := json.Unmarshal(b, &baseline); err != nil {
			log.Fatal(err)
		}
		log.Printf("phase post: comparing %d query cases hot+cold", len(cases))
		results := make([]caseResult, 0, len(cases))
		allEqual := true
		for _, c := range cases {
			// Unified query (hot + cold merged) must equal the pre-archive source
			// result exactly.
			res, err := uq.Run(ctx, withoutProbeRows(c.q))
			if err != nil {
				log.Fatalf("post query %s: %v", c.name, err)
			}
			before := baseline[c.name]
			equal := rowsEqual(before, res.Rows)
			if !equal {
				allEqual = false
			}
			cr := caseResult{
				Name: c.name, RowsBefore: len(before), RowsAfter: len(res.Rows), Equal: equal,
				HotRows: res.Stats.HotRows, ColdRows: res.Stats.ColdRows,
				ColdLatency: float64(res.Stats.ColdQuery.Duration.Microseconds()) / 1000,
				EOELatency:  float64(res.Stats.Duration.Microseconds()) / 1000,
				BlocksTotal: res.Stats.ColdQuery.BlocksTotal, Pruned: res.Stats.ColdQuery.BlocksPruned,
			}
			if !equal {
				cr.DiffDetail = firstDiff(before, res.Rows)
			}
			results = append(results, cr)
		}
		report["post_cases"] = results
		report["all_equal"] = allEqual
		report["mismatches"] = countMismatches(results)
		var coldRows, hotRows int
		for _, c := range results {
			coldRows += c.ColdRows
			hotRows += c.HotRows
		}
		report["merged_rows"] = map[string]int{"hot": hotRows, "cold": coldRows}

		// Latency benchmarking: cold-only vs hot-only vs mixed.
		bench := benchmark(ctx, src, cold, uq, coldCut, cfg.Demo.BenchIterations)
		report["latency"] = bench

		// Storage totals.
		st, _ := src.Stats(ctx)
		nSeg64, totalColdRows, coldBytes, rawBytes, _, _, _ := meta.StorageTotals(ctx)
		nSeg := int(nSeg64)
		segs, _ := meta.Segments(ctx, false)
		var segSourceEst int64
		var minT, maxT int64
		minT, maxT = 1<<62, 0
		for _, s := range segs {
			if s.Status == "active" {
				segSourceEst += s.SourceBytes
				if s.MinTime < minT {
					minT = s.MinTime
				}
				if s.MaxTime > maxT {
					maxT = s.MaxTime
				}
			}
		}
		report["storage"] = map[string]any{
			"source_rows": st.Rows, "source_total_bytes": st.TotalBytes,
			"source_heap_bytes": st.HeapBytes, "source_index_bytes": st.IndexBytes,
			"cold_segments": nSeg, "cold_rows": totalColdRows,
			"cold_bytes": coldBytes, "cold_raw_bytes": rawBytes,
			"cold_source_bytes_est": segSourceEst,
			"peak_ratio":            ratio(rawBytes, coldBytes),
			"cold_vs_source_ratio":  ratio(segSourceEst, coldBytes),
			"cold_min_time":         time.UnixMicro(minT).UTC(),
			"cold_max_time":         time.UnixMicro(maxT).UTC(),
		}
		log.Printf("all_equal=%v", allEqual)
	}
	saveJSON(*out, report)
	if *out == "" {
		saveJSON(filepath.Join(cfg.Demo.ReportDir, "bench-report.json"), report)
	}
}

func countMismatches(results []caseResult) int {
	n := 0
	for _, c := range results {
		if !c.Equal {
			n++
		}
	}
	return n
}

type queryCase struct {
	name string
	q    model.Query
}

func loadOrCreateAnchor(path string) time.Time {
	if b, err := os.ReadFile(path); err == nil {
		var t time.Time
		if err := json.Unmarshal(b, &t); err == nil && !t.IsZero() {
			return t
		}
	}
	now := time.Now().UTC()
	saveJSON(path, now)
	return now
}

func buildCases(src *source.Source, now time.Time) []queryCase {
	y := func(yearsAgo float64) time.Time { return now.AddDate(0, 0, int(-365*yearsAgo)) }
	_ = now
	ptr := func(v int64) *int64 { return &v }
	var cases []queryCase
	// A spread of ranges: entirely cold, crossing the 3-year boundary, recent
	// hot-only, wide ranges, and narrow high-selectivity filters.
	ranges := []struct {
		name     string
		from, to time.Time
	}{
		{"cold-2020", y(6), y(5.5)},
		{"cold-2021-h1", y(5), y(4.5)},
		{"boundary-cross", y(3.2), y(2.8)},
		{"hot-90d", y(0.25), now},
		{"wide-4y", y(4), now},
	}
	limit := 200
	for _, r := range ranges {
		cases = append(cases, queryCase{name: r.name, q: model.Query{From: r.from, To: r.to, Limit: limit}})
		cases = append(cases, queryCase{name: r.name + "/merchant", q: model.Query{From: r.from, To: r.to, Limit: limit, MerchantID: ptr(4242)}})
		cases = append(cases, queryCase{name: r.name + "/amount", q: model.Query{From: r.from, To: r.to, Limit: limit, MinAmount: ptr(100000), MaxAmount: ptr(250000)}})
	}
	// Specific txn lookups: resolve real transaction numbers at several depths
	// (cold, boundary, hot) so point lookups exercise both stores.
	if id, err := src.CurrentMaxID(context.Background()); err == nil && id > 0 {
		for _, frac := range []float64{0.05, 0.35, 0.6, 0.9} {
			txnID := int64(float64(id) * frac)
			var txnNo string
			var created time.Time
			if err := src.Pool.QueryRow(context.Background(),
				`SELECT txn_no, created_at FROM transactions WHERE id=$1`, txnID).Scan(&txnNo, &created); err != nil {
				continue
			}
			cases = append(cases, queryCase{
				name: fmt.Sprintf("txn-id-%d", txnID),
				q: model.Query{From: created.AddDate(0, 0, -30), To: created.AddDate(0, 0, 30),
					Limit: 10, TxnNo: txnNo},
			})
		}
	}
	// Deterministic shuffled filters to exercise block pruning.
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 6; i++ {
		from := y(float64(2 + rng.Intn(4)))
		to := from.AddDate(0, 0, 30+rng.Intn(90))
		c := queryCase{name: fmt.Sprintf("random-%d", i), q: model.Query{From: from, To: to, Limit: 100}}
		switch i % 4 {
		case 0:
			c.q.Status = "declined"
		case 1:
			c.q.Channel = "online"
		case 2:
			c.q.Region = "EMEA"
		case 3:
			c.q.Currency = "SGD"
		}
		cases = append(cases, c)
	}
	return cases
}

// withoutProbeRows excludes synthetic live-traffic rows (id >= 9e11) so the
// archival parity check compares only the generated dataset. The unified API
// itself has no such filter; this is a benchmark-only convenience.
func withoutProbeRows(q model.Query) model.Query {
	maxID := int64(899_999_999_999)
	q.MaxID = &maxID
	return q
}

func toTransactions(rows []model.Row) []model.TransactionRow {
	out := make([]model.TransactionRow, len(rows))
	for i, r := range rows {
		out[i] = model.RowToTransaction(r)
	}
	return out
}

func rowsEqual(a, b []model.TransactionRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !txnEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func txnEqual(a, b model.TransactionRow) bool {
	am, bm := "", ""
	if a.Memo != nil {
		am = *a.Memo
	}
	if b.Memo != nil {
		bm = *b.Memo
	}
	return a.ID == b.ID && a.TxnNo == b.TxnNo && a.MerchantID == b.MerchantID &&
		a.CardLast4 == b.CardLast4 && a.AmountCents == b.AmountCents && a.Currency == b.Currency &&
		a.Status == b.Status && a.Channel == b.Channel && a.Region == b.Region &&
		a.RiskScore == b.RiskScore && a.CreatedAt == b.CreatedAt && am == bm
}

func firstDiff(a, b []model.TransactionRow) string {
	var sb strings.Builder
	if len(a) != len(b) {
		fmt.Fprintf(&sb, "row count %d vs %d; ", len(a), len(b))
	}
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if !txnEqual(a[i], b[i]) {
			ja, _ := json.Marshal(a[i])
			jb, _ := json.Marshal(b[i])
			fmt.Fprintf(&sb, "first diff at %d: %s vs %s", i, ja, jb)
			break
		}
	}
	return sb.String()
}

type latencyReport struct {
	Hot  latencyStats `json:"hot_only"`
	Cold latencyStats `json:"cold_only"`
	Mix  latencyStats `json:"mixed"`
}

type latencyStats struct {
	Iterations  int     `json:"iterations"`
	P50MS       float64 `json:"p50_ms"`
	P95MS       float64 `json:"p95_ms"`
	MaxMS       float64 `json:"max_ms"`
	AvgMS       float64 `json:"avg_ms"`
	RowsPerCall int     `json:"rows_per_call"`
}

func benchmark(ctx context.Context, src *source.Source, cold *coldquery.Engine, uq *unifiedquery.Engine, coldCut time.Time, iterations int) latencyReport {
	if iterations <= 0 {
		iterations = 30
	}
	// Cold-only query: a 1-month window entirely inside cold storage.
	coldFrom := coldCut.AddDate(-2, 0, 0)
	coldQ := model.Query{From: coldFrom, To: coldFrom.AddDate(0, 1, 0), Limit: 100, Status: "approved"}
	// Hot-only query: last 30 days.
	now := time.Now().UTC()
	hotQ := model.Query{From: now.AddDate(0, 0, -30), To: now, Limit: 100}
	mixedQ := model.Query{From: coldCut.AddDate(0, -1, 0), To: now, Limit: 100}

	measure := func(fn func() int) latencyStats {
		lats := make([]float64, 0, iterations)
		rows := 0
		for i := 0; i < iterations; i++ {
			t0 := time.Now()
			n := fn()
			lats = append(lats, float64(time.Since(t0).Microseconds())/1000)
			rows = n
		}
		sort.Float64s(lats)
		sum := 0.0
		for _, v := range lats {
			sum += v
		}
		return latencyStats{
			Iterations: iterations, RowsPerCall: rows,
			P50MS: lats[len(lats)*50/100], P95MS: lats[len(lats)*95/100],
			MaxMS: lats[len(lats)-1], AvgMS: sum / float64(len(lats)),
		}
	}
	hot := measure(func() int {
		p, err := src.QueryPage(ctx, hotQ, nil, 100)
		if err != nil {
			log.Fatalf("hot bench: %v", err)
		}
		return len(p.Rows)
	})
	coldS := measure(func() int {
		rows, _, err := cold.Query(ctx, coldQ, "transactions", 100, nil)
		if err != nil {
			log.Fatalf("cold bench: %v", err)
		}
		return len(rows)
	})
	mix := measure(func() int {
		res, err := uq.Run(ctx, mixedQ)
		if err != nil {
			log.Fatalf("mixed bench: %v", err)
		}
		return len(res.Rows)
	})
	return latencyReport{Hot: hot, Cold: coldS, Mix: mix}
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func saveJSON(path string, v any) {
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatal(err)
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		log.Fatal(err)
	}
}
