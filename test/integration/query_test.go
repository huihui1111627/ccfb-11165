package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"payment-archive/internal/coldquery"
	"payment-archive/internal/model"
)

// TestPaginationParity verifies cursor pagination spans hot and cold without
// gaps or duplicates.
func TestPaginationParity(t *testing.T) {
	a, _ := newTestApp(t)
	ctx := context.Background()
	p := addPolicy(t, a, 1095, 1825)
	now := time.Now().UTC()
	// Build the full expected sequence by paging the *source* before archiving,
	// which is the ground truth for the post-archive unified pager.
	var want []model.TransactionRow
	var srcCursor *model.Key
	for {
		page, err := a.Source.QueryPage(ctx, model.Query{From: now.AddDate(-7, 0, 0), To: now}, srcCursor, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page.Rows {
			want = append(want, model.RowToTransaction(r))
		}
		if !page.HasMore || len(page.Rows) == 0 {
			break
		}
		last := page.Rows[len(page.Rows)-1]
		k := model.Key{Time: time.UnixMicro(last[10].I), ID: last[0].I}
		srcCursor = &k
	}
	if len(want) < 500 {
		t.Fatalf("dataset too small for pagination test: %d", len(want))
	}
	job, err := a.Enqueue(ctx, "archive", p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j := waitJob(t, a, job.ID); j.Status != "succeeded" {
		t.Fatalf("archive: %s", j.Error)
	}

	// Page through with a small limit and compare the concatenation.
	var got []model.TransactionRow
	var cursor *model.Cursor
	pages := 0
	for {
		q := model.Query{From: now.AddDate(-7, 0, 0), To: now, Limit: 37, Cursor: cursor}
		res, err := a.Unified.Run(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, res.Rows...)
		pages++
		if !res.HasMore || res.Next == nil {
			break
		}
		cursor = res.Next
	}
	if pages < 5 {
		t.Fatalf("expected several pages, got %d", pages)
	}
	if len(got) != len(want) {
		t.Fatalf("pagination returned %d rows, want %d", len(got), len(want))
	}
	seen := map[string]bool{}
	for i := range got {
		key := fmt.Sprintf("%s|%d", got[i].CreatedAt, got[i].ID)
		if seen[key] {
			t.Fatalf("duplicate row %s on page boundary", key)
		}
		seen[key] = true
		if !equalRows([]model.TransactionRow{got[i]}, []model.TransactionRow{want[i]}) {
			t.Fatalf("row %d differs: got %+v want %+v", i, got[i], want[i])
		}
	}
}

// TestColdQueryAdmissionControl checks that the cold layer rejects excess
// concurrency and QPS instead of dropping correctness.
func TestColdQueryAdmissionControl(t *testing.T) {
	a, _ := newTestApp(t)
	ctx := context.Background()
	p := addPolicy(t, a, 1095, 1825)
	job, _ := a.Enqueue(ctx, "archive", p.ID)
	if j := waitJob(t, a, job.ID); j.Status != "succeeded" {
		t.Fatalf("archive: %s", j.Error)
	}
	// A dedicated engine with tight limits so the assertion is deterministic.
	cfg := a.Cfg.ColdQuery
	cfg.MaxConcurrent = 1
	cfg.MaxQPS = 1
	cfg.QueueTimeoutMS = 1
	ce, err := coldquery.New(cfg, a.Meta)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	q := model.Query{From: now.AddDate(-7, 0, 0), To: now.AddDate(-4, 0, 0), Limit: 10}
	var throttled, ok int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := ce.Query(ctx, q, "transactions", 10, nil)
			if err != nil {
				if _, isT := err.(*coldquery.ThrottledError); isT {
					atomic.AddInt64(&throttled, 1)
					return
				}
				t.Errorf("unexpected error: %v", err)
				return
			}
			atomic.AddInt64(&ok, 1)
		}()
	}
	wg.Wait()
	if throttled == 0 {
		t.Fatalf("expected some queries to be throttled (ok=%d)", ok)
	}
	if ok == 0 {
		t.Fatalf("expected at least one query to succeed")
	}
	if got := ce.Metrics().Throttled.Load(); got != throttled {
		t.Fatalf("metrics throttled=%d, observed=%d", got, throttled)
	}
}

// TestPolicyValidation ensures the destructive-retention guard is enforced by
// the destruction engine even if a policy bypasses the HTTP validator.
func TestPolicyValidation(t *testing.T) {
	a, cfg := newTestApp(t)
	ctx := context.Background()
	p := addPolicy(t, a, 1095, 1825)
	// Directly lower retention below the configured regulatory floor.
	if _, err := a.Meta.DB().ExecContext(ctx, `UPDATE policies SET destroy_after_days=? WHERE id=?`,
		cfg.RegulatoryMinRetentionDay-1, p.ID); err != nil {
		t.Fatal(err)
	}
	job, err := a.Enqueue(ctx, "destroy", p.ID)
	if err != nil {
		t.Fatal(err)
	}
	j := waitJob(t, a, job.ID)
	if j.Status != "failed" {
		t.Fatalf("destroy below regulatory floor must fail, got %s", j.Status)
	}
	if j.Error == "" {
		t.Fatal("expected an explanatory error")
	}
}
