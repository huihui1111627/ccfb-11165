package integration

import (
	"context"
	"testing"
	"time"

	"payment-archive/internal/model"
)

// TestArchiveWhileUpdating verifies that rows updated during archival are not
// lost: the row keeps its latest version in one of the stores and the unified
// query returns exactly one copy.
func TestArchiveWhileUpdating(t *testing.T) {
	a, _ := newTestApp(t)
	ctx := context.Background()
	p := addPolicy(t, a, 1095, 1825)

	now := time.Now().UTC()
	// Pick a batch of cold rows and update a few of them just before the
	// archive snapshot. updated_at moves to now, so the fence excludes them;
	// after the next run they must still be present exactly once.
	rows, err := a.Source.QueryPage(ctx, model.Query{
		From: now.AddDate(-4, 0, 0), To: now.AddDate(-3, 0, 0),
	}, nil, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) < 10 {
		t.Fatal("not enough cold rows")
	}
	ids := make([]int64, 0, 10)
	for _, r := range rows.Rows[:10] {
		ids = append(ids, r[0].I)
	}
	if _, err := a.Source.Pool.Exec(ctx,
		`UPDATE transactions SET status='refunded' WHERE id = ANY($1)`, ids); err != nil {
		t.Fatal(err)
	}
	// Wait a moment so now() > archive snapshot fence candidate.
	time.Sleep(20 * time.Millisecond)
	job, err := a.Enqueue(ctx, "archive", p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j := waitJob(t, a, job.ID); j.Status != "succeeded" {
		t.Fatalf("archive: %s", j.Error)
	}

	// Run a second archive so the updated rows (now old enough and no longer
	// excluded by updated_at) migrate too. This creates the duplicate-key case
	// only if a fresh copy is written while an older copy exists; here it just
	// proves the fence logic does not lose rows.
	time.Sleep(20 * time.Millisecond)
	job2, err := a.Enqueue(ctx, "archive", p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j := waitJob(t, a, job2.ID); j.Status != "succeeded" {
		t.Fatalf("archive2: %s", j.Error)
	}

	q := model.Query{From: now.AddDate(-4, 0, 0), To: now.AddDate(-3, 0, 0), Limit: 1000}
	res, err := a.Unified.Run(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int64]int{}
	for _, r := range res.Rows {
		counts[r.ID]++
		if counts[r.ID] > 1 {
			t.Fatalf("row %d returned more than once (duplicate across stores)", r.ID)
		}
	}
	for _, id := range ids {
		if counts[id] != 1 {
			t.Fatalf("updated row %d missing from unified result (count=%d)", id, counts[id])
		}
	}
	// The updated status must be the visible one.
	srcRows, err := a.Source.FetchRowsByID(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if row, ok := srcRows[id]; ok {
			if row[6].S != "refunded" {
				t.Fatalf("source row %d has stale status %q", id, row[6].S)
			}
		}
	}
	// Cold rows with the same id, if any, must also carry the refunded status.
	for _, r := range res.Rows {
		for _, id := range ids {
			if r.ID == id && r.Status != "refunded" {
				t.Fatalf("unified row %d returned stale status %q", id, r.Status)
			}
		}
	}
}

// TestSchedulerIdempotent ensures repeated archive runs do not duplicate rows.
func TestSchedulerIdempotent(t *testing.T) {
	a, _ := newTestApp(t)
	ctx := context.Background()
	p := addPolicy(t, a, 1095, 1825)
	now := time.Now().UTC()
	q := model.Query{From: now.AddDate(-7, 0, 0), To: now, Limit: 1000}
	before := queryRows(t, a, q)
	for i := 0; i < 2; i++ {
		j, err := a.Enqueue(ctx, "archive", p.ID)
		if err != nil {
			t.Fatal(err)
		}
		if jj := waitJob(t, a, j.ID); jj.Status != "succeeded" {
			t.Fatalf("archive %d: %s", i, jj.Error)
		}
	}
	after := queryRows(t, a, q)
	if !equalRows(before, after) {
		t.Fatalf("repeat archive changed results: %d vs %d rows", len(before), len(after))
	}
	segs, _ := a.Meta.ActiveSegments(ctx, "transactions")
	var total int64
	for _, s := range segs {
		total += s.Rows
	}
	var coldRows int64
	if _, n, _, _, _, _, err := a.Meta.StorageTotals(ctx); err == nil {
		coldRows = n
	}
	if coldRows != total {
		t.Fatalf("cold totals mismatch")
	}
}
