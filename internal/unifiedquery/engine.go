// Package unifiedquery merges hot (source) and cold (segment) results behind
// a single API so callers do not need to know where data lives.
package unifiedquery

import (
	"context"
	"fmt"
	"sort"
	"time"

	"payment-archive/internal/coldquery"
	"payment-archive/internal/model"
	"payment-archive/internal/source"
)

type Engine struct {
	Source *source.Source
	Cold   *coldquery.Engine
	// MaxLimit caps the caller-provided limit.
	MaxLimit     int
	DefaultLimit int
	Table        string
}

type Result struct {
	Rows    []model.TransactionRow `json:"rows"`
	Limit   int                    `json:"limit"`
	Next    *model.Cursor          `json:"next_cursor,omitempty"`
	HasMore bool                   `json:"has_more"`
	Stats   Stats                  `json:"stats"`
}

type Stats struct {
	HotRows   int             `json:"hot_rows"`
	ColdRows  int             `json:"cold_rows"`
	Hot       bool            `json:"hot_consulted"`
	Cold      bool            `json:"cold_consulted"`
	ColdQuery coldquery.Stats `json:"cold_query"`
	// Kept indicates rows dropped because they existed in both stores at the
	// same key (only possible during an in-flight migration).
	Deduped  int           `json:"deduped"`
	Duration time.Duration `json:"duration"`
}

// Run executes the unified query. The time range is split between source and
// cold; results are merged, deduplicated by (created_at,id) and sorted.
func (e *Engine) Run(ctx context.Context, q model.Query) (Result, error) {
	start := time.Now()
	if err := q.Validate(); err != nil {
		return Result{}, err
	}
	limit := q.Limit
	if limit <= 0 {
		limit = e.DefaultLimit
	}
	if limit > e.MaxLimit {
		return Result{}, fmt.Errorf("limit exceeds maximum of %d", e.MaxLimit)
	}
	if e.MaxLimit <= 0 {
		e.MaxLimit = 1000
	}
	var cursor *model.Key
	if q.Cursor != nil {
		k := q.Cursor.Key()
		cursor = &k
	}

	// Hot path always runs: it covers recent rows and any archived rows still
	// present in the source (held rows stay in the source by design).
	hot, err := e.Source.QueryPage(ctx, q, cursor, limit)
	if err != nil {
		return Result{}, fmt.Errorf("hot query: %w", err)
	}

	// Cold path: skip entirely when the query range is newer than the newest
	// cold segment (common for recent transactions).
	cold, coldStats, err := e.Cold.Query(ctx, q, e.Table, limit, cursor)
	if err != nil {
		if te, ok := err.(*coldquery.ThrottledError); ok {
			// Fail closed? No: cold data is required for correctness, so surface
			// the throttling to the caller rather than returning partial data.
			return Result{}, &ThrottledError{Reason: te.Reason}
		}
		return Result{}, fmt.Errorf("cold query: %w", err)
	}

	// Merge.
	merged := make([]model.Row, 0, len(hot.Rows)+len(cold))
	seen := make(map[string]struct{}, len(hot.Rows)+len(cold))
	deduped := 0
	add := func(rows []model.Row) {
		for _, r := range rows {
			key := fmt.Sprintf("%d|%d", r[10].I, r[0].I)
			if _, ok := seen[key]; ok {
				deduped++
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, r)
		}
	}
	add(hot.Rows)
	add(cold)
	sort.Slice(merged, func(i, j int) bool {
		if merged[i][10].I != merged[j][10].I {
			return merged[i][10].I > merged[j][10].I
		}
		return merged[i][0].I > merged[j][0].I
	})
	coldMore := len(cold) >= limit
	hasMore := len(merged) > limit || hot.HasMore || coldMore
	if len(merged) > limit {
		merged = merged[:limit]
	}
	var next *model.Cursor
	if len(merged) > 0 && hasMore {
		last := merged[len(merged)-1]
		next = &model.Cursor{Time: time.UnixMicro(last[10].I).UTC(), ID: last[0].I}
	}
	trs := make([]model.TransactionRow, len(merged))
	for i, r := range merged {
		trs[i] = model.RowToTransaction(r)
	}
	return Result{
		Rows:    trs,
		Limit:   limit,
		Next:    next,
		HasMore: hasMore,
		Stats: Stats{
			HotRows:   len(hot.Rows),
			ColdRows:  len(cold),
			Hot:       true,
			Cold:      true,
			ColdQuery: coldStats,
			Deduped:   deduped,
			Duration:  time.Since(start),
		},
	}, nil
}

// ThrottledError is returned when the cold layer rejects the query.
type ThrottledError struct{ Reason string }

func (e *ThrottledError) Error() string { return e.Reason }
