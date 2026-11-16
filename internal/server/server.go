// Package server exposes the management API and static UI.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"payment-archive/internal/app"
	"payment-archive/internal/coldquery"
	"payment-archive/internal/config"
	"payment-archive/internal/control"
	"payment-archive/internal/model"
	"payment-archive/internal/unifiedquery"
	"payment-archive/web"
)

type Server struct {
	App *app.App
	Cfg config.Config
}

func New(a *app.App, cfg config.Config) *Server { return &Server{App: a, Cfg: cfg} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/status", s.status)
	mux.HandleFunc("GET /api/policies", s.listPolicies)
	mux.HandleFunc("POST /api/policies", s.createPolicy)
	mux.HandleFunc("PUT /api/policies/{id}", s.updatePolicy)
	mux.HandleFunc("POST /api/policies/{id}/archive", s.runArchive)
	mux.HandleFunc("POST /api/policies/{id}/destroy", s.runDestroy)
	mux.HandleFunc("GET /api/jobs", s.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", s.cancelJob)
	mux.HandleFunc("POST /api/jobs/{id}/verify", s.verifyJob)
	mux.HandleFunc("GET /api/storage", s.storage)
	mux.HandleFunc("GET /api/query", s.query)
	mux.HandleFunc("GET /api/holds", s.listHolds)
	mux.HandleFunc("POST /api/holds", s.createHold)
	mux.HandleFunc("POST /api/holds/{id}/release", s.releaseHold)
	mux.HandleFunc("GET /api/manifests", s.listManifests)
	mux.HandleFunc("GET /api/manifests/{id}", s.getManifest)
	mux.HandleFunc("GET /api/manifests/{id}/download", s.downloadManifest)
	mux.HandleFunc("GET /api/holds/impact", s.holdImpact)
	mux.HandleFunc("GET /api/report", s.report)
	sub, err := fs.Sub(web.Files, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))
	return logging(mux)
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		if !strings.HasPrefix(r.URL.Path, "/assets") {
			log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, sw.status, time.Since(start).Round(time.Millisecond))
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]any{"error": err.Error()})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true, "time": time.Now().UTC()})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcStats, err := s.App.Source.Stats(ctx)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	nSeg, coldRows, coldBytes, rawBytes, minT, maxT, err := s.App.Meta.StorageTotals(ctx)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	blocks, cacheBytes, hits, misses := s.App.Cold.CacheStats()
	jobs, _ := s.App.Meta.Jobs(ctx, 5)
	writeJSON(w, 200, map[string]any{
		"source": srcStats,
		"cold": map[string]any{
			"segments":     nSeg,
			"rows":         coldRows,
			"bytes":        coldBytes,
			"raw_bytes":    rawBytes,
			"min_time":     minT,
			"max_time":     maxT,
			"cache_blocks": blocks,
			"cache_bytes":  cacheBytes,
			"cache_hits":   hits,
			"cache_misses": misses,
		},
		"recent_jobs": jobs,
		"config": map[string]any{
			"cold_after_days":    s.Cfg.Demo.ColdAfterDays,
			"destroy_after_days": s.Cfg.Demo.DestroyAfterDay,
			"regulatory_min":     s.Cfg.RegulatoryMinRetentionDay,
		},
		"cold_query_metrics": snapMetrics(s.App.Cold.Metrics()),
	})
}

func snapMetrics(m *coldquery.Metrics) map[string]any {
	total := m.Total.Load()
	avgUS := int64(0)
	if total > 0 {
		avgUS = m.TotalLatencyUS.Load() / total
	}
	return map[string]any{
		"total":          total,
		"throttled":      m.Throttled.Load(),
		"failed":         m.Failed.Load(),
		"rows_served":    m.RowsServed.Load(),
		"blocks_scanned": m.BlocksScanned.Load(),
		"blocks_pruned":  m.BlocksPruned.Load(),
		"bytes_read":     m.BytesRead.Load(),
		"avg_latency_ms": float64(avgUS) / 1000,
		"max_latency_ms": float64(m.MaxLatencyUS.Load()) / 1000,
	}
}

func (s *Server) listPolicies(w http.ResponseWriter, r *http.Request) {
	p, err := s.App.Meta.Policies(r.Context())
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	type policyView struct {
		control.Policy
		ColdCandidates int64 `json:"cold_candidates"`
		SourceRows     int64 `json:"source_rows"`
		DestroyReady   int64 `json:"destroy_ready_segments"`
	}
	out := make([]policyView, 0, len(p))
	for _, pol := range p {
		v := policyView{Policy: pol}
		if n, err := s.App.Source.CountCold(r.Context(), pol.ColdAfterDays); err == nil {
			v.ColdCandidates = n
		}
		if st, err := s.App.Source.Stats(r.Context()); err == nil {
			v.SourceRows = st.Rows
		}
		segs, err := s.App.Meta.ActiveSegments(r.Context(), pol.TableName)
		if err == nil {
			cut := time.Now().UTC().AddDate(0, 0, -pol.DestroyAfterDays).UnixMicro()
			for _, seg := range segs {
				if seg.MaxTime < cut {
					v.DestroyReady++
				}
			}
		}
		out = append(out, v)
	}
	writeJSON(w, 200, out)
}

type policyReq struct {
	TableName         string `json:"table_name"`
	TimeField         string `json:"time_field"`
	PKField           string `json:"pk_field"`
	ColdAfterDays     int    `json:"cold_after_days"`
	DestroyAfterDays  int    `json:"destroy_after_days"`
	BatchRows         int    `json:"batch_rows"`
	TargetSegmentRows int    `json:"target_segment_rows"`
	Enabled           bool   `json:"enabled"`
	Description       string `json:"description"`
}

func (s *Server) validatePolicy(p policyReq) error {
	if _, err := control.NormalizeTableName(p.TableName); err != nil {
		return err
	}
	if p.TableName != "transactions" {
		return fmt.Errorf("only the transactions table is supported by this deployment")
	}
	if p.ColdAfterDays < 1 {
		return fmt.Errorf("cold_after_days must be >= 1")
	}
	if p.DestroyAfterDays < s.Cfg.RegulatoryMinRetentionDay {
		return fmt.Errorf("destroy_after_days must be at least the regulatory minimum of %d days", s.Cfg.RegulatoryMinRetentionDay)
	}
	if p.DestroyAfterDays <= p.ColdAfterDays {
		return fmt.Errorf("destroy_after_days must be greater than cold_after_days")
	}
	if p.BatchRows < 1000 || p.BatchRows > 500000 {
		return fmt.Errorf("batch_rows must be between 1000 and 500000")
	}
	if p.TargetSegmentRows < p.BatchRows || p.TargetSegmentRows > 20_000_000 {
		return fmt.Errorf("target_segment_rows must be >= batch_rows and <= 20000000")
	}
	return nil
}

func (s *Server) createPolicy(w http.ResponseWriter, r *http.Request) {
	var req policyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)
		return
	}
	if req.BatchRows == 0 {
		req.BatchRows = s.Cfg.Archive.BatchRows
	}
	if req.TargetSegmentRows == 0 {
		req.TargetSegmentRows = s.Cfg.Archive.TargetSegmentRows
	}
	if err := s.validatePolicy(req); err != nil {
		writeErr(w, 400, err)
		return
	}
	p := &control.Policy{
		TableName: req.TableName, TimeField: orDefault(req.TimeField, "created_at"),
		PKField: orDefault(req.PKField, "id"), ColdAfterDays: req.ColdAfterDays,
		DestroyAfterDays: req.DestroyAfterDays, BatchRows: req.BatchRows,
		TargetSegmentRows: req.TargetSegmentRows, Enabled: req.Enabled, Description: req.Description,
	}
	if err := s.App.Meta.CreatePolicy(r.Context(), p); err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 201, p)
}

func (s *Server) updatePolicy(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	p, err := s.App.Meta.Policy(r.Context(), id)
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	var req policyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)
		return
	}
	req.TableName, req.TimeField, req.PKField = p.TableName, p.TimeField, p.PKField
	if err := s.validatePolicy(req); err != nil {
		writeErr(w, 400, err)
		return
	}
	p.ColdAfterDays, p.DestroyAfterDays = req.ColdAfterDays, req.DestroyAfterDays
	p.BatchRows, p.TargetSegmentRows = req.BatchRows, req.TargetSegmentRows
	p.Enabled, p.Description = req.Enabled, req.Description
	if err := s.App.Meta.UpdatePolicy(r.Context(), p); err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, p)
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func (s *Server) runArchive(w http.ResponseWriter, r *http.Request) {
	s.enqueue(w, r, "archive")
}

func (s *Server) runDestroy(w http.ResponseWriter, r *http.Request) {
	s.enqueue(w, r, "destroy")
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request, typ string) {
	job, err := s.App.Enqueue(r.Context(), typ, pathID(r))
	if err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 202, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	limit := intParam(r, "limit", 50)
	jobs, err := s.App.Meta.Jobs(r.Context(), limit)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, jobs)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	job, err := s.App.Meta.Job(r.Context(), id)
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	logs, _ := s.App.Meta.JobLogs(r.Context(), id, 300)
	writeJSON(w, 200, map[string]any{"job": job, "logs": logs})
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := s.App.Cancel(pathID(r)); err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 202, map[string]any{"ok": true})
}

func (s *Server) verifyJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.App.Meta.CreateJob(r.Context(), "verify", 0)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	// Verification of all segments is done inline (read-only, no source writes).
	go func() {
		if err := s.App.Archive.VerifyJob(r.Context(), pathID(r)); err != nil {
			_ = s.App.Meta.FinishJob(r.Context(), job.ID, "failed", err.Error())
			return
		}
		_ = s.App.Meta.FinishJob(r.Context(), job.ID, "succeeded", "")
	}()
	writeJSON(w, 202, job)
}

func (s *Server) storage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	src, err := s.App.Source.Stats(ctx)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	segs, err := s.App.Meta.Segments(ctx, false)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	var activeRows, activeBytes, activeRaw, activeSrc int64
	buckets := map[string]*dayBucket{}
	var segRows []map[string]any
	for _, seg := range segs {
		if seg.Status != "active" {
			continue
		}
		activeRows += seg.Rows
		activeBytes += seg.SizeBytes
		activeRaw += seg.RawBytes
		activeSrc += seg.SourceBytes
		day := time.UnixMicro(seg.MinTime).UTC().Format("2006-01")
		b := buckets[day]
		if b == nil {
			b = &dayBucket{Day: day}
			buckets[day] = b
		}
		b.Segments++
		b.Rows += seg.Rows
		b.Bytes += seg.SizeBytes
		b.RawBytes += seg.RawBytes
		b.SourceEst += seg.SourceBytes
		segRows = append(segRows, map[string]any{
			"id": seg.ID, "path": seg.Path, "rows": seg.Rows, "size_bytes": seg.SizeBytes,
			"raw_bytes": seg.RawBytes, "sha256": seg.SHA256, "min_time": seg.MinTime,
			"max_time": seg.MaxTime, "created_at": seg.CreatedAt, "verified_at": seg.VerifiedAt,
		})
	}
	bucketList := make([]*dayBucket, 0, len(buckets))
	for _, b := range buckets {
		bucketList = append(bucketList, b)
	}
	sortBuckets(bucketList)
	ratio := 0.0
	if activeBytes > 0 {
		ratio = float64(activeSrc) / float64(activeBytes)
	}
	writeJSON(w, 200, map[string]any{
		"source": src,
		"cold": map[string]any{
			"segments": len(segRows), "rows": activeRows, "bytes": activeBytes,
			"raw_bytes": activeRaw, "source_bytes_est": activeSrc, "ratio": ratio,
		},
		"by_month": bucketList,
		"segments": segRows,
	})
}

type dayBucket struct {
	Day       string `json:"day"`
	Segments  int    `json:"segments"`
	Rows      int64  `json:"rows"`
	Bytes     int64  `json:"bytes"`
	RawBytes  int64  `json:"raw_bytes"`
	SourceEst int64  `json:"source_bytes_est"`
}

func sortBuckets(b []*dayBucket) {
	sort.Slice(b, func(i, j int) bool { return b[i].Day < b[j].Day })
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	q, err := parseQuery(r)
	if err != nil {
		writeErr(w, 400, err)
		return
	}
	res, err := s.App.Unified.Run(r.Context(), q)
	if err != nil {
		var te *unifiedquery.ThrottledError
		if errors.As(err, &te) {
			w.Header().Set("Retry-After", "1")
			writeErr(w, 429, err)
			return
		}
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, res)
}

func parseQuery(r *http.Request) (model.Query, error) {
	var q model.Query
	vals := r.URL.Query()
	from, err := parseTimeParam(vals.Get("from"))
	if err != nil {
		return q, fmt.Errorf("from: %w", err)
	}
	to, err := parseTimeParam(vals.Get("to"))
	if err != nil {
		return q, fmt.Errorf("to: %w", err)
	}
	q.From, q.To = from, to
	q.Status = vals.Get("status")
	q.Channel = vals.Get("channel")
	q.Region = vals.Get("region")
	q.Currency = vals.Get("currency")
	q.TxnNo = vals.Get("txn_no")
	if v := vals.Get("merchant_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return q, fmt.Errorf("merchant_id: %w", err)
		}
		q.MerchantID = &n
	}
	if v := vals.Get("min_amount"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return q, fmt.Errorf("min_amount: %w", err)
		}
		q.MinAmount = &n
	}
	if v := vals.Get("max_amount"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return q, fmt.Errorf("max_amount: %w", err)
		}
		q.MaxAmount = &n
	}
	if v := vals.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return q, fmt.Errorf("limit: %w", err)
		}
		q.Limit = n
	}
	if v := vals.Get("cursor"); v != "" {
		c, err := decodeCursor(v)
		if err != nil {
			return q, fmt.Errorf("cursor: %w", err)
		}
		q.Cursor = c
	}
	return q, nil
}

func parseTimeParam(v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, errors.New("required")
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t.UTC(), nil
	}
	if d, err := time.ParseDuration(v); err == nil {
		return time.Now().UTC().Add(-d), nil
	}
	return time.Time{}, fmt.Errorf("expected RFC3339 timestamp or duration like 720h")
}

func encodeCursor(c *model.Cursor) string {
	return fmt.Sprintf("%d.%d", c.Time.UnixMicro(), c.ID)
}

func decodeCursor(v string) (*model.Cursor, error) {
	parts := strings.SplitN(v, ".", 2)
	if len(parts) != 2 {
		return nil, errors.New("malformed cursor")
	}
	us, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	return &model.Cursor{Time: time.UnixMicro(us).UTC(), ID: id}, nil
}

func (s *Server) listHolds(w http.ResponseWriter, r *http.Request) {
	holds, err := s.App.Meta.Holds(r.Context(), false)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, holds)
}

type holdReq struct {
	CaseNo      string     `json:"case_no"`
	Reason      string     `json:"reason"`
	Scope       string     `json:"scope"`
	TargetValue string     `json:"target_value"`
	TargetFrom  int64      `json:"target_from"`
	TargetTo    int64      `json:"target_to"`
	TimeFrom    *time.Time `json:"time_from"`
	TimeTo      *time.Time `json:"time_to"`
	CreatedBy   string     `json:"created_by"`
}

func (s *Server) createHold(w http.ResponseWriter, r *http.Request) {
	var req holdReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)
		return
	}
	if strings.TrimSpace(req.CaseNo) == "" {
		writeErr(w, 400, errors.New("case_no is required"))
		return
	}
	switch req.Scope {
	case model.HoldTxn, model.HoldMerchant:
		if req.TargetValue == "" {
			writeErr(w, 400, errors.New("target_value is required for this scope"))
			return
		}
	case model.HoldTxnRange:
		if req.TargetFrom <= 0 || req.TargetTo < req.TargetFrom {
			writeErr(w, 400, errors.New("target_from/target_to must be a valid id range"))
			return
		}
	case model.HoldTimeRange:
		if req.TimeFrom == nil && req.TimeTo == nil {
			writeErr(w, 400, errors.New("time_from or time_to is required"))
			return
		}
	default:
		writeErr(w, 400, fmt.Errorf("unknown scope %q", req.Scope))
		return
	}
	h := &model.Hold{
		CaseNo: req.CaseNo, Reason: req.Reason, Scope: req.Scope, TargetValue: req.TargetValue,
		TargetFrom: req.TargetFrom, TargetTo: req.TargetTo, TimeFrom: req.TimeFrom, TimeTo: req.TimeTo,
		CreatedBy: orDefault(req.CreatedBy, "console"),
	}
	if err := s.App.Meta.CreateHold(r.Context(), h); err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 201, h)
}

func (s *Server) releaseHold(w http.ResponseWriter, r *http.Request) {
	if err := s.App.Meta.ReleaseHold(r.Context(), pathID(r)); err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// holdImpact estimates how many cold rows and segments are protected by holds.
func (s *Server) holdImpact(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	holds, err := s.App.Meta.Holds(ctx, true)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	segs, err := s.App.Meta.ActiveSegments(ctx, "transactions")
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	type perHold struct {
		HoldID int64  `json:"hold_id"`
		CaseNo string `json:"case_no"`
		Rows   int64  `json:"cold_rows"`
	}
	per := make([]perHold, len(holds))
	for i, h := range holds {
		per[i] = perHold{HoldID: h.ID, CaseNo: h.CaseNo}
	}
	writeJSON(w, 200, map[string]any{
		"active_holds": len(holds),
		"segments":     len(segs),
		"note":         "Exact row counts per hold are computed during destruction; this endpoint reports scope.",
		"holds":        per,
	})
}

func (s *Server) listManifests(w http.ResponseWriter, r *http.Request) {
	m, err := s.App.Meta.Manifests(r.Context(), intParam(r, "limit", 50))
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, m)
}

func (s *Server) getManifest(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	m, err := s.App.Meta.Manifest(r.Context(), id)
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	segs, _ := s.App.Meta.ManifestSegments(r.Context(), id)
	doc, _ := os.ReadFile(m.FilePath)
	writeJSON(w, 200, map[string]any{"manifest": m, "segments": segs, "document": json.RawMessage(doc)})
}

func (s *Server) downloadManifest(w http.ResponseWriter, r *http.Request) {
	m, err := s.App.Meta.Manifest(r.Context(), pathID(r))
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", strings.TrimSuffix(strings.TrimPrefix(m.FilePath, s.Cfg.DataDir+"/manifests/"), ".json")+".json"))
	http.ServeFile(w, r, m.FilePath)
}

func (s *Server) report(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	src, err := s.App.Source.Stats(ctx)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	nSeg, coldRows, coldBytes, rawBytes, _, _, err := s.App.Meta.StorageTotals(ctx)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	jobs, _ := s.App.Meta.Jobs(ctx, 20)
	manifests, _ := s.App.Meta.Manifests(ctx, 20)
	holds, _ := s.App.Meta.Holds(ctx, true)
	writeJSON(w, 200, map[string]any{
		"generated_at": time.Now().UTC(),
		"source":       src,
		"cold": map[string]any{
			"segments": nSeg, "rows": coldRows, "bytes": coldBytes, "raw_bytes": rawBytes,
		},
		"jobs": jobs, "manifests": manifests, "active_holds": holds,
		"cold_query_metrics": snapMetrics(s.App.Cold.Metrics()),
	})
}

func pathID(r *http.Request) int64 {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	return id
}

func intParam(r *http.Request, name string, def int) int {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
