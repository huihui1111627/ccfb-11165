// Package source adapts the operational Postgres database. All archival reads
// and source-side deletions go through this package.
package source

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"payment-archive/internal/model"
)

type Source struct {
	Pool *pgxpool.Pool
	// SeedStart is the earliest created_at used by the generator.
	SeedStart time.Time
	// SeedEnd is the latest created_at used by the generator.
	SeedEnd time.Time
}

func Open(ctx context.Context, dsn string) (*Source, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 16
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Source{Pool: pool}, nil
}

func (s *Source) Close() { s.Pool.Close() }

const ddl = `
CREATE TABLE IF NOT EXISTS transactions (
	id           bigint PRIMARY KEY,
	txn_no       text NOT NULL,
	merchant_id  bigint NOT NULL,
	card_last4   text NOT NULL,
	amount_cents bigint NOT NULL,
	currency     text NOT NULL,
	status       text NOT NULL,
	channel      text NOT NULL,
	region       text NOT NULL,
	risk_score   bigint NOT NULL,
	created_at   timestamptz NOT NULL,
	memo         text,
	updated_at   timestamptz NOT NULL DEFAULT now(),
	archived_at  timestamptz
);
CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at, id);
CREATE INDEX IF NOT EXISTS idx_transactions_archivable ON transactions(created_at, id) WHERE archived_at IS NULL;

CREATE OR REPLACE FUNCTION transactions_touch_updated() RETURNS trigger AS $$
BEGIN
	IF (NEW.txn_no, NEW.merchant_id, NEW.card_last4, NEW.amount_cents, NEW.currency,
	    NEW.status, NEW.channel, NEW.region, NEW.risk_score, NEW.created_at, NEW.memo)
	   IS DISTINCT FROM
	   (OLD.txn_no, OLD.merchant_id, OLD.card_last4, OLD.amount_cents, OLD.currency,
	    OLD.status, OLD.channel, OLD.region, OLD.risk_score, OLD.created_at, OLD.memo)
	THEN
		NEW.updated_at := now();
		NEW.archived_at := NULL;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_transactions_touch ON transactions;
CREATE TRIGGER trg_transactions_touch BEFORE UPDATE ON transactions
	FOR EACH ROW EXECUTE FUNCTION transactions_touch_updated();
`

// EnsureSchema creates the demo transaction table if needed.
func (s *Source) EnsureSchema(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, ddl)
	return err
}

// TableStats describes source-side storage.
type TableStats struct {
	Rows           int64     `json:"rows"`
	TotalBytes     int64     `json:"total_bytes"`
	HeapBytes      int64     `json:"heap_bytes"`
	IndexBytes     int64     `json:"index_bytes"`
	MinTime        time.Time `json:"min_time"`
	MaxTime        time.Time `json:"max_time"`
	ArchivedRows   int64     `json:"archived_rows"`
	PendingArchive int64     `json:"pending_archive"`
}

func (s *Source) Stats(ctx context.Context) (TableStats, error) {
	var st TableStats
	err := s.Pool.QueryRow(ctx, `SELECT
		COALESCE(pg_total_relation_size('transactions'),0),
		COALESCE(pg_relation_size('transactions'),0),
		COALESCE(pg_indexes_size('transactions'),0),
		COUNT(*), MIN(created_at), MAX(created_at),
		COUNT(*) FILTER (WHERE archived_at IS NOT NULL)
		FROM transactions`).Scan(&st.TotalBytes, &st.HeapBytes, &st.IndexBytes, &st.Rows, &st.MinTime, &st.MaxTime, &st.ArchivedRows)
	if err != nil {
		return st, err
	}
	st.PendingArchive = st.Rows - st.ArchivedRows
	return st, nil
}

// Seed generates rows deterministically using generate_series. It is intended
// for the demo dataset; the encoding below is designed to be compressible in a
// realistic way while still carrying skew.
func (s *Source) Seed(ctx context.Context, rows int64, years int) error {
	start := time.Now().UTC().AddDate(-years, 0, 0).Truncate(time.Second)
	end := time.Now().UTC().Truncate(time.Second)
	s.SeedStart, s.SeedEnd = start, end
	spanUS := end.Sub(start).Microseconds()
	if rows <= 0 {
		rows = 30_000_000
	}
	stepUS := spanUS / rows
	if stepUS < 1 {
		stepUS = 1
	}
	if _, err := s.Pool.Exec(ctx, `SET synchronous_commit=off`); err != nil {
		return err
	}
	// Avoid double-seeding.
	var n int64
	if err := s.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions`).Scan(&n); err != nil {
		return err
	}
	if err := s.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions WHERE id > $1`, rows).Scan(&n); err != nil {
		return err
	}
	// Delete anything beyond target (e.g. from a previous larger seed).
	if _, err := s.Pool.Exec(ctx, `DELETE FROM transactions WHERE id > $1`, rows); err != nil {
		return err
	}
	if err := s.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions`).Scan(&n); err != nil {
		return err
	}
	if n >= rows {
		return nil
	}
	// Fill missing ids in chunks to keep transactions bounded.
	const chunk = 2_000_000
	for from := int64(1); from <= rows; from += chunk {
		to := from + chunk - 1
		if to > rows {
			to = rows
		}
		_, err := s.Pool.Exec(ctx, `
INSERT INTO transactions
	(id, txn_no, merchant_id, card_last4, amount_cents, currency, status, channel, region, risk_score, created_at, memo, updated_at)
SELECT g,
	'TXN' || lpad(g::text, 12, '0'),
	1 + (g * 7919) % 5000,
	lpad(((g * 104729) % 10000)::text, 4, '0'),
	100 + (g * 2654435761) % 500000,
	(CASE WHEN g % 10 < 5 THEN 'USD' WHEN g % 10 < 7 THEN 'EUR' WHEN g % 10 < 9 THEN 'GBP' ELSE 'SGD' END),
	(CASE WHEN g % 100 < 85 THEN 'approved' WHEN g % 100 < 93 THEN 'declined' WHEN g % 100 < 98 THEN 'refunded' ELSE 'pending' END),
	(CASE WHEN g % 4 = 0 THEN 'pos' WHEN g % 4 = 1 THEN 'online' WHEN g % 4 = 2 THEN 'mobile' ELSE 'atm' END),
	(CASE WHEN g % 10 < 4 THEN 'APAC' WHEN g % 10 < 7 THEN 'EMEA' WHEN g % 10 < 9 THEN 'AMER' ELSE 'LATAM' END),
	(g * 37) % 100,
	$1::timestamptz + (((g - 1) * $2::bigint + ((g * 2654435761) % 1000000) - 500000)::text || ' microseconds')::interval,
	(CASE WHEN g % 10 < 3 THEN 'settlement batch ' || ((g % 1000))::text ELSE NULL END),
	$1::timestamptz + (((g - 1) * $2::bigint)::text || ' microseconds')::interval
FROM generate_series($3::bigint, $4::bigint) g
ON CONFLICT (id) DO NOTHING`, start, stepUS, from, to)
		if err != nil {
			return fmt.Errorf("seed rows %d-%d: %w", from, to, err)
		}
	}
	_, err := s.Pool.Exec(ctx, `ANALYZE transactions`)
	return err
}

// ColdSnapshot is the state of rows eligible for a cold migration job.
type ColdSnapshot struct {
	ColdCutoff time.Time
	Fence      time.Time
	TotalRows  int64
	MinTime    time.Time
	MaxTime    time.Time
}

// SnapshotCold counts archive candidates older than the cold cutoff.
func (s *Source) SnapshotCold(ctx context.Context, coldAfterDays int) (ColdSnapshot, error) {
	now := time.Now().UTC()
	cutoff := now.AddDate(0, 0, -coldAfterDays)
	snap := ColdSnapshot{ColdCutoff: cutoff, Fence: now}
	err := s.Pool.QueryRow(ctx, `SELECT COUNT(*), COALESCE(MIN(created_at),$1), COALESCE(MAX(created_at),$1)
		FROM transactions WHERE created_at < $2 AND archived_at IS NULL AND updated_at <= $3`,
		now, cutoff, now).Scan(&snap.TotalRows, &snap.MinTime, &snap.MaxTime)
	return snap, err
}

// CountCold counts candidates (used by the scheduler/UI).
func (s *Source) CountCold(ctx context.Context, coldAfterDays int) (int64, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -coldAfterDays)
	var n int64
	err := s.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions WHERE created_at < $1 AND archived_at IS NULL`, cutoff).Scan(&n)
	return n, err
}

// StreamBatch reads one keyset page of archive candidates in ascending
// (created_at,id) order. The caller supplies the last key from the previous
// page. Nil firstTime means start from the beginning.
func (s *Source) StreamBatch(ctx context.Context, snap ColdSnapshot, afterTime *time.Time, afterID int64, limit int) ([]model.Row, *time.Time, int64, error) {
	if limit <= 0 {
		limit = 50000
	}
	var rows pgx.Rows
	var err error
	if afterTime == nil {
		rows, err = s.Pool.Query(ctx, `
SELECT id, txn_no, merchant_id, card_last4, amount_cents, currency, status, channel, region, risk_score, created_at, memo
FROM transactions
WHERE created_at < $1 AND archived_at IS NULL AND updated_at <= $2
ORDER BY created_at ASC, id ASC
LIMIT $3`, snap.ColdCutoff, snap.Fence, limit)
	} else {
		rows, err = s.Pool.Query(ctx, `
SELECT id, txn_no, merchant_id, card_last4, amount_cents, currency, status, channel, region, risk_score, created_at, memo
FROM transactions
WHERE created_at < $1 AND archived_at IS NULL AND updated_at <= $2
  AND (created_at, id) > ($3, $4)
ORDER BY created_at ASC, id ASC
LIMIT $5`, snap.ColdCutoff, snap.Fence, *afterTime, afterID, limit)
	}
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()
	out := make([]model.Row, 0, limit)
	var lastTime time.Time
	var lastID int64
	for rows.Next() {
		row := make(model.Row, 12)
		var created time.Time
		var memo *string
		if err := rows.Scan(&row[0].I, &row[1].S, &row[2].I, &row[3].S, &row[4].I, &row[5].S,
			&row[6].S, &row[7].S, &row[8].S, &row[9].I, &created, &memo); err != nil {
			return nil, nil, 0, err
		}
		row[10].I = created.UTC().UnixMicro()
		if memo == nil {
			row[11].Null = true
		} else {
			row[11].S = *memo
		}
		out = append(out, row)
		lastTime, lastID = created, row[0].I
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	if len(out) == 0 {
		return out, nil, 0, nil
	}
	lt := lastTime
	return out, &lt, lastID, nil
}

// HoldPredicate builds a SQL boolean expression that is TRUE for rows under an
// active legal hold. Returns "false" when no holds are active.
func HoldPredicate(holds []model.Hold) string {
	if len(holds) == 0 {
		return "false"
	}
	parts := make([]string, 0, len(holds))
	for _, h := range holds {
		switch h.Scope {
		case model.HoldTxn:
			parts = append(parts, fmt.Sprintf("txn_no = %s", quote(h.TargetValue)))
		case model.HoldTxnRange:
			parts = append(parts, fmt.Sprintf("id BETWEEN %d AND %d", h.TargetFrom, h.TargetTo))
		case model.HoldMerchant:
			parts = append(parts, fmt.Sprintf("merchant_id = %s", quote(h.TargetValue)))
		case model.HoldTimeRange:
			// Both bounds belong to the same hold and must be combined with AND,
			// otherwise an open-ended bound would match every row.
			var bounds []string
			if h.TimeFrom != nil {
				bounds = append(bounds, fmt.Sprintf("created_at >= %s", quote(h.TimeFrom.UTC().Format(time.RFC3339Nano))))
			}
			if h.TimeTo != nil {
				bounds = append(bounds, fmt.Sprintf("created_at < %s", quote(h.TimeTo.UTC().Format(time.RFC3339Nano))))
			}
			if len(bounds) == 0 {
				parts = append(parts, "true")
			} else {
				parts = append(parts, "("+strings.Join(bounds, " AND ")+")")
			}
		default:
			parts = append(parts, "true")
		}
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// DeleteStats reports the outcome of source-side eviction for one segment.
type DeleteStats struct {
	Deleted        int64
	HeldMarked     int64
	Chunks         int
	MaxChunkMillis int64
	Watermark      int64
}

// EvictSegment deletes source rows that are safely archived in a cold segment.
// Rows under legal hold are retained and marked archived so they are not
// re-archived. Deletion walks the segment range with a keyset cursor and
// deletes small id batches, which keeps each transaction short and avoids the
// O(n^2) rescans of an offset-style loop.
func (s *Source) EvictSegment(ctx context.Context, segMinTime, segMaxTime, segMinID, segMaxID int64, cutoffMS int64, fenceMS int64, holds []model.Hold, chunkRows int, pause time.Duration, progress func(DeleteStats)) (DeleteStats, error) {
	var ds DeleteStats
	minT := time.UnixMicro(segMinTime).UTC()
	maxT := time.UnixMicro(segMaxTime).UTC()
	cutoff := time.UnixMilli(cutoffMS).UTC()
	fence := time.UnixMilli(fenceMS).UTC()
	holdSQL := HoldPredicate(holds)
	holdSet := model.HoldSet(holds)

	// Mark retained (held) rows so they are not archived again. This is a
	// single indexed range update; held rows are typically a small fraction.
	if len(holds) > 0 {
		tag, err := s.Pool.Exec(ctx, `
UPDATE transactions SET archived_at = now()
WHERE (created_at, id) >= ($1, $2) AND (created_at, id) <= ($3, $4)
  AND created_at < $5 AND updated_at <= $6 AND archived_at IS NULL AND `+holdSQL,
			minT, segMinID, maxT, segMaxID, cutoff, fence)
		if err != nil {
			return ds, fmt.Errorf("mark held: %w", err)
		}
		ds.HeldMarked = tag.RowsAffected()
	}

	if chunkRows <= 0 {
		chunkRows = 2000
	}
	curTime, curID := minT, segMinID-1
	for {
		if err := ctx.Err(); err != nil {
			return ds, err
		}
		scanStart := time.Now()
		rows, err := s.Pool.Query(ctx, `
SELECT id, txn_no, merchant_id, created_at
FROM transactions
WHERE (created_at, id) > ($1, $2) AND (created_at, id) <= ($3, $4)
  AND created_at < $5 AND updated_at <= $6
ORDER BY created_at ASC, id ASC
LIMIT $7`, curTime, curID, maxT, segMaxID, cutoff, fence, chunkRows)
		if err != nil {
			return ds, fmt.Errorf("scan chunk: %w", err)
		}
		type ref struct {
			id  int64
			txn string
			mid int64
			t   time.Time
		}
		batch := make([]ref, 0, chunkRows)
		for rows.Next() {
			var r ref
			if err := rows.Scan(&r.id, &r.txn, &r.mid, &r.t); err != nil {
				rows.Close()
				return ds, err
			}
			batch = append(batch, r)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return ds, err
		}
		rows.Close()
		if len(batch) == 0 {
			break
		}
		last := batch[len(batch)-1]
		curTime, curID = last.t, last.id

		var del []int64
		for _, r := range batch {
			if holdSet.MatchesFields(r.id, r.txn, r.mid, r.t) {
				continue
			}
			del = append(del, r.id)
		}
		if len(del) > 0 {
			tag, err := s.Pool.Exec(ctx, `DELETE FROM transactions WHERE id = ANY($1)`, del)
			if err != nil {
				return ds, fmt.Errorf("delete batch: %w", err)
			}
			ds.Deleted += tag.RowsAffected()
		}
		dur := time.Since(scanStart).Milliseconds()
		if dur > ds.MaxChunkMillis {
			ds.MaxChunkMillis = dur
		}
		ds.Chunks++
		ds.Watermark = last.t.UnixMicro()
		if progress != nil {
			progress(ds)
		}
		if len(batch) < chunkRows {
			break
		}
		if pause > 0 {
			select {
			case <-time.After(pause):
			case <-ctx.Done():
				return ds, ctx.Err()
			}
		}
	}
	return ds, nil
}

// DeleteExpiredArchived removes rows past the destruction cutoff that were
// already archived and are no longer under hold (e.g. a released hold).
func (s *Source) DeleteExpiredArchived(ctx context.Context, cutoff time.Time, holds []model.Hold, chunkRows int, pause time.Duration) (int64, error) {
	if chunkRows <= 0 {
		chunkRows = 2000
	}
	holdSQL := HoldPredicate(holds)
	var total int64
	for {
		tag, err := s.Pool.Exec(ctx, `
DELETE FROM transactions WHERE id IN (
  SELECT id FROM transactions
  WHERE archived_at IS NOT NULL AND created_at < $1 AND NOT `+holdSQL+`
  LIMIT $2)`, cutoff, chunkRows)
		if err != nil {
			return total, err
		}
		n := tag.RowsAffected()
		total += n
		if n < int64(chunkRows) {
			break
		}
		if pause > 0 {
			select {
			case <-time.After(pause):
			case <-ctx.Done():
				return total, ctx.Err()
			}
		}
	}
	return total, nil
}

// QueryPage fetches hot rows in descending (created_at,id) order. When cursor
// is non-nil, only rows strictly before the cursor are returned.
type HotPage struct {
	Rows    []model.Row
	HasMore bool
}

func (s *Source) QueryPage(ctx context.Context, q model.Query, cursor *model.Key, limit int) (HotPage, error) {
	var rows pgx.Rows
	var err error
	args := []any{q.From, q.To}
	where := `created_at >= $1 AND created_at <= $2`
	if q.MinAmount != nil {
		args = append(args, *q.MinAmount)
		where += fmt.Sprintf(" AND amount_cents >= $%d", len(args))
	}
	if q.MaxAmount != nil {
		args = append(args, *q.MaxAmount)
		where += fmt.Sprintf(" AND amount_cents <= $%d", len(args))
	}
	if q.MerchantID != nil {
		args = append(args, *q.MerchantID)
		where += fmt.Sprintf(" AND merchant_id = $%d", len(args))
	}
	if q.Status != "" {
		args = append(args, q.Status)
		where += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if q.Channel != "" {
		args = append(args, q.Channel)
		where += fmt.Sprintf(" AND channel = $%d", len(args))
	}
	if q.Region != "" {
		args = append(args, q.Region)
		where += fmt.Sprintf(" AND region = $%d", len(args))
	}
	if q.Currency != "" {
		args = append(args, q.Currency)
		where += fmt.Sprintf(" AND currency = $%d", len(args))
	}
	if q.TxnNo != "" {
		args = append(args, q.TxnNo)
		where += fmt.Sprintf(" AND txn_no = $%d", len(args))
	}
	if q.MaxID != nil {
		args = append(args, *q.MaxID)
		where += fmt.Sprintf(" AND id <= $%d", len(args))
	}
	if cursor != nil {
		args = append(args, cursor.Time, cursor.ID)
		where += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	sql := `SELECT id, txn_no, merchant_id, card_last4, amount_cents, currency, status, channel, region, risk_score, created_at, memo
FROM transactions WHERE ` + where + fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))
	rows, err = s.Pool.Query(ctx, sql, args...)
	if err != nil {
		return HotPage{}, err
	}
	defer rows.Close()
	page := HotPage{Rows: make([]model.Row, 0, limit)}
	for rows.Next() {
		row := make(model.Row, 12)
		var created time.Time
		var memo *string
		if err := rows.Scan(&row[0].I, &row[1].S, &row[2].I, &row[3].S, &row[4].I, &row[5].S,
			&row[6].S, &row[7].S, &row[8].S, &row[9].I, &created, &memo); err != nil {
			return page, err
		}
		row[10].I = created.UTC().UnixMicro()
		if memo == nil {
			row[11].Null = true
		} else {
			row[11].S = *memo
		}
		page.Rows = append(page.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return page, err
	}
	if len(page.Rows) > limit {
		page.HasMore = true
		page.Rows = page.Rows[:limit]
	}
	return page, nil
}

// FetchRowsByID returns source rows keyed by id (used by the verifier).
func (s *Source) FetchRowsByID(ctx context.Context, ids []int64) (map[int64]model.Row, error) {
	out := make(map[int64]model.Row, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT id, txn_no, merchant_id, card_last4, amount_cents, currency, status, channel, region, risk_score, created_at, memo FROM transactions WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		row := make(model.Row, 12)
		var created time.Time
		var memo *string
		if err := rows.Scan(&row[0].I, &row[1].S, &row[2].I, &row[3].S, &row[4].I, &row[5].S,
			&row[6].S, &row[7].S, &row[8].S, &row[9].I, &created, &memo); err != nil {
			return nil, err
		}
		row[10].I = created.UTC().UnixMicro()
		if memo == nil {
			row[11].Null = true
		} else {
			row[11].S = *memo
		}
		out[row[0].I] = row
	}
	return out, rows.Err()
}

// CurrentMaxID returns the highest transaction id.
func (s *Source) CurrentMaxID(ctx context.Context) (int64, error) {
	var id int64
	err := s.Pool.QueryRow(ctx, `SELECT COALESCE(MAX(id),0) FROM transactions`).Scan(&id)
	return id, err
}
