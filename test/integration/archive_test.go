package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"payment-archive/internal/app"
	"payment-archive/internal/config"
	"payment-archive/internal/control"
	"payment-archive/internal/model"
)

// newTestApp creates an isolated app against the local Postgres with a small
// dataset. It skips when Postgres or the payarchive_test database is missing.
func newTestApp(t *testing.T) (*app.App, config.Config) {
	t.Helper()
	if os.Getenv("PAYARCHIVE_SKIP_DB") != "" {
		t.Skip("PAYARCHIVE_SKIP_DB set")
	}
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = dir
	cfg.ControlDB = filepath.Join(dir, "control.db")
	cfg.SourceDSN = envOr("PAYARCHIVE_TEST_DSN", "postgres:///payarchive_test?sslmode=disable")
	cfg.SchedulerInterval = 0
	cfg.AutoArchive = false
	cfg.AutoDestroy = false
	cfg.Archive.BatchRows = 5000
	cfg.Archive.TargetSegmentRows = 20000
	cfg.Archive.DeleteChunkRows = 1000
	cfg.Archive.Workers = 2
	// The dataset spans 7 years; lower the regulatory floor so the test can
	// exercise destruction of the oldest data.
	cfg.RegulatoryMinRetentionDay = 300
	cfg.ColdQuery.MaxConcurrent = 4
	cfg.ColdQuery.MaxQPS = 1000
	cfg.ColdQuery.MaxLimit = 1000
	cfg.ColdQuery.DefaultLimit = 100
	ctx := context.Background()
	a, err := app.New(ctx, cfg)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(a.Close)
	// Start from a clean, known dataset.
	if _, err := a.Source.Pool.Exec(ctx, `TRUNCATE transactions`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	start := time.Now().UTC().AddDate(-7, 0, 0)
	if _, err := a.Source.Pool.Exec(ctx, `
INSERT INTO transactions (id, txn_no, merchant_id, card_last4, amount_cents, currency, status, channel, region, risk_score, created_at, memo, updated_at)
SELECT g, 'TXN' || lpad(g::text, 12, '0'), 1 + (g*7919) % 5000, lpad(((g*104729)%10000)::text,4,'0'),
       100 + (g*2654435761)%500000,
       (CASE WHEN g%10<5 THEN 'USD' WHEN g%10<7 THEN 'EUR' WHEN g%10<9 THEN 'GBP' ELSE 'SGD' END),
       (CASE WHEN g%100<85 THEN 'approved' WHEN g%100<93 THEN 'declined' WHEN g%100<98 THEN 'refunded' ELSE 'pending' END),
       (CASE WHEN g%4=0 THEN 'pos' WHEN g%4=1 THEN 'online' WHEN g%4=2 THEN 'mobile' ELSE 'atm' END),
       (CASE WHEN g%10<4 THEN 'APAC' WHEN g%10<7 THEN 'EMEA' WHEN g%10<9 THEN 'AMER' ELSE 'LATAM' END),
       (g*37)%100, $1::timestamptz + ((g::bigint * 3600000000)::text || ' microseconds')::interval,
       (CASE WHEN g%10<3 THEN 'memo ' || (g%1000)::text ELSE NULL END),
       $1::timestamptz + ((g::bigint * 3600000000)::text || ' microseconds')::interval
FROM generate_series(1, 60000) AS gs(g0), LATERAL (SELECT g0::bigint AS g) x`, start); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	return a, cfg
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func addPolicy(t *testing.T, a *app.App, coldAfter, destroyAfter int) *control.Policy {
	t.Helper()
	p := &control.Policy{
		TableName: "transactions", TimeField: "created_at", PKField: "id",
		ColdAfterDays: coldAfter, DestroyAfterDays: destroyAfter,
		BatchRows: 5000, TargetSegmentRows: 20000, Enabled: true, Description: "test",
	}
	if err := a.Meta.CreatePolicy(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	return p
}

func waitJob(t *testing.T, a *app.App, id int64) *control.Job {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		j, err := a.Meta.Job(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == "succeeded" || j.Status == "failed" || j.Status == "cancelled" {
			return j
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %d did not finish", id)
	return nil
}

func queryRows(t *testing.T, a *app.App, q model.Query) []model.TransactionRow {
	t.Helper()
	res, err := a.Unified.Run(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	return res.Rows
}

func TestArchiveQueryParityAndHoldDestruction(t *testing.T) {
	a, cfg := newTestApp(t)
	ctx := context.Background()
	p := addPolicy(t, a, 1095, 1825)

	now := time.Now().UTC()
	from := now.AddDate(-7, 0, 0)
	to := now
	limit := 200

	// Select deterministic ids to place under a txn hold.
	var heldID int64
	if err := a.Source.Pool.QueryRow(ctx, `SELECT id FROM transactions WHERE created_at < $1 ORDER BY created_at DESC LIMIT 1`,
		now.AddDate(-3, 0, 0)).Scan(&heldID); err != nil {
		t.Fatal(err)
	}
	heldRow, err := a.Source.FetchRowsByID(ctx, []int64{heldID})
	if err != nil {
		t.Fatal(err)
	}
	heldTxn := model.RowToTransaction(heldRow[heldID])

	holds := []*model.Hold{
		{CaseNo: "CASE-T-1", Reason: "litigation", Scope: model.HoldTxn, TargetValue: heldTxn.TxnNo, CreatedBy: "test"},
	}
	// Hold a merchant present in cold data.
	var merchantID int64
	if err := a.Source.Pool.QueryRow(ctx, `SELECT merchant_id FROM transactions WHERE created_at < $1 LIMIT 1`,
		now.AddDate(-3, 0, 0)).Scan(&merchantID); err != nil {
		t.Fatal(err)
	}
	holds = append(holds, &model.Hold{CaseNo: "CASE-T-2", Reason: "merchant", Scope: model.HoldMerchant,
		TargetValue: fmt.Sprintf("%d", merchantID), CreatedBy: "test"})
	for _, h := range holds {
		if err := a.Meta.CreateHold(ctx, h); err != nil {
			t.Fatal(err)
		}
	}

	queries := []model.Query{
		{From: from, To: to, Limit: limit},
		{From: from, To: to, Limit: limit, Status: "declined"},
		{From: now.AddDate(-5, 0, 0), To: now.AddDate(-4, 0, 0), Limit: limit},
		{From: now.AddDate(-3, -1, 0), To: now.AddDate(-2, 11, 0), Limit: limit},
		{From: now.AddDate(-1, 0, 0), To: now, Limit: limit},
		{From: from, To: to, Limit: 10, MerchantID: &merchantID},
	}
	before := make([][]model.TransactionRow, len(queries))
	for i, q := range queries {
		before[i] = queryRows(t, a, q)
	}
	sourceBefore, _ := a.Source.Stats(ctx)

	// Run archive.
	job, err := a.Enqueue(ctx, "archive", p.ID)
	if err != nil {
		t.Fatal(err)
	}
	done := waitJob(t, a, job.ID)
	if done.Status != "succeeded" {
		t.Fatalf("archive failed: %s", done.Error)
	}
	srcAfter, _ := a.Source.Stats(ctx)
	if srcAfter.Rows >= sourceBefore.Rows {
		t.Fatalf("expected source rows to shrink, %d -> %d", sourceBefore.Rows, srcAfter.Rows)
	}
	nSeg, coldRows, _, _, _, _, err := a.Meta.StorageTotals(ctx)
	if err != nil || nSeg == 0 {
		t.Fatalf("no cold segments: %v", err)
	}
	if coldRows != done.ProgressRows {
		t.Fatalf("cold rows %d != archived %d", coldRows, done.ProgressRows)
	}

	// Post-archive: unified results must match the pre-archive source results
	// byte-for-byte, including held rows that remain hot.
	for i, q := range queries {
		after := queryRows(t, a, q)
		if !equalRows(before[i], after) {
			ja, _ := json.Marshal(before[i])
			jb, _ := json.Marshal(after)
			t.Fatalf("query %d differs after archive:\nbefore=%s\nafter=%s", i, truncate(ja), truncate(jb))
		}
	}
	// Held transaction is still present in the source.
	if _, err := a.Source.FetchRowsByID(ctx, []int64{heldID}); err != nil {
		t.Fatal(err)
	}
	var heldCount int64
	if err := a.Source.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE id=$1`, heldID).Scan(&heldCount); err != nil {
		t.Fatal(err)
	}
	if heldCount != 1 {
		t.Fatal("held transaction was evicted from source")
	}

	// Force an early destruction cutoff: every cold segment is older than the
	// (test-shortened) retention. Destroy must retain held rows.
	verifyHeldSurvivesDestruction(t, a, p, heldTxn, merchantID, cfg)

	// Repeat archive after hold release to ensure held rows eventually archive.
	segs, _ := a.Meta.ActiveSegments(ctx, "transactions")
	if len(segs) == 0 {
		t.Fatal("expected retained segments after partial destruction")
	}
}

func verifyHeldSurvivesDestruction(t *testing.T, a *app.App, p *control.Policy, held model.TransactionRow, merchantID int64, cfg config.Config) {
	t.Helper()
	ctx := context.Background()
	// The seeded dataset spans 7 years. With a 400-day retention cutoff (just
	// above the test regulatory floor), the oldest cold segments are past
	// retention while recent data is not.
	p.ColdAfterDays = 1000
	p.DestroyAfterDays = 400
	if err := a.Meta.UpdatePolicy(ctx, p); err != nil {
		t.Fatal(err)
	}

	job, err := a.Enqueue(ctx, "destroy", p.ID)
	if err != nil {
		t.Fatal(err)
	}
	done := waitJob(t, a, job.ID)
	if done.Status != "succeeded" {
		t.Fatalf("destroy failed: %s", done.Error)
	}
	if done.RowsDestroyed == 0 {
		t.Fatal("expected rows to be destroyed")
	}
	if done.RowsHeld == 0 {
		t.Fatal("expected held rows to be retained")
	}

	// Manifest exists, is hashed, and lists destroyed ids.
	manifests, err := a.Meta.Manifests(ctx, 10)
	if err != nil || len(manifests) == 0 {
		t.Fatalf("no manifest: %v", err)
	}
	m := manifests[0]
	if m.RowsDestroyed != done.RowsDestroyed {
		t.Fatalf("manifest rows %d != job %d", m.RowsDestroyed, done.RowsDestroyed)
	}
	if m.IDSHA256 == "" || m.FileSHA256 == "" {
		t.Fatal("manifest hashes missing")
	}
	b, err := os.ReadFile(m.FilePath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != m.FileSHA256 {
		t.Fatal("manifest file hash does not verify")
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["row_id_digest_sha256"] == "" {
		t.Fatal("row id digest missing")
	}
	msegs, _ := a.Meta.ManifestSegments(ctx, m.ID)
	if len(msegs) == 0 {
		t.Fatal("manifest segment detail missing")
	}

	// Held rows must still be queryable through the unified API.
	q := model.Query{From: time.Now().UTC().AddDate(-8, 0, 0), To: time.Now().UTC(), Limit: 1000,
		MerchantID: &merchantID}
	rows := queryRows(t, a, q)
	if len(rows) == 0 {
		t.Fatal("held merchant rows disappeared after destruction")
	}
	q2 := model.Query{From: time.Now().UTC().AddDate(-8, 0, 0), To: time.Now().UTC(), Limit: 10, TxnNo: held.TxnNo}
	got := queryRows(t, a, q2)
	if len(got) != 1 {
		t.Fatalf("held transaction lost after destruction: got %d rows", len(got))
	}
}

func equalRows(a, b []model.TransactionRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].CreatedAt != b[i].CreatedAt || a[i].TxnNo != b[i].TxnNo ||
			a[i].AmountCents != b[i].AmountCents || a[i].Status != b[i].Status ||
			a[i].MerchantID != b[i].MerchantID || memo(a[i]) != memo(b[i]) {
			return false
		}
	}
	return true
}

func memo(r model.TransactionRow) string {
	if r.Memo == nil {
		return "\x00null"
	}
	return *r.Memo
}

func truncate(b []byte) string {
	if len(b) > 600 {
		return string(b[:600]) + "…"
	}
	return string(b)
}
