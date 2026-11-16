// Package archive implements cold migration: batched reads from the source,
// columnar segment writing, verification, metadata registration and chunked
// source eviction.
package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"payment-archive/internal/cold"
	"payment-archive/internal/config"
	"payment-archive/internal/control"
	"payment-archive/internal/model"
	"payment-archive/internal/source"
)

// Hooks lets the web layer observe progress without importing the engine.
type Hooks struct {
	Log        func(level, msg string)
	Progress   func(j *control.Job)
	OnVerified func(seg *control.Segment)
}

type Engine struct {
	Cfg    config.Config
	Source *source.Source
	Meta   *control.Store
	Hooks  Hooks
}

func New(cfg config.Config, src *source.Source, meta *control.Store) *Engine {
	return &Engine{Cfg: cfg, Source: src, Meta: meta}
}

func (e *Engine) logf(jobID int64, level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if e.Hooks.Log != nil {
		e.Hooks.Log(level, msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.Meta.AddLog(ctx, jobID, level, msg)
}

func (e *Engine) progress(j *control.Job) {
	if e.Hooks.Progress == nil {
		return
	}
	e.Hooks.Progress(j)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.Meta.UpdateJob(ctx, j)
}

func (e *Engine) segDir(table string) string {
	return filepath.Join(e.Cfg.DataDir, "cold", table)
}

// Run executes an archive job to completion. It is safe to run concurrently
// with source writes: reads use a snapshot fence and source deletes are chunked.
func (e *Engine) Run(ctx context.Context, job *control.Job, policy *control.Policy) error {
	if err := e.Meta.StartJob(ctx, job.ID); err != nil {
		return err
	}
	job.Status, job.Phase = "running", "snapshot"
	e.progress(job)

	holds, err := e.Meta.Holds(ctx, true)
	if err != nil {
		return e.fail(ctx, job, err)
	}
	snap, err := e.Source.SnapshotCold(ctx, policy.ColdAfterDays)
	if err != nil {
		return e.fail(ctx, job, fmt.Errorf("snapshot: %w", err))
	}
	job.TotalRows = snap.TotalRows
	// Measure the real heap+index footprint per row before eviction so the
	// compression report compares like with like.
	bytesPerRow := int64(90)
	if st, err := e.Source.Stats(ctx); err == nil && st.Rows > 0 {
		bytesPerRow = (st.HeapBytes + st.IndexBytes) / st.Rows
		if bytesPerRow <= 0 {
			bytesPerRow = 90
		}
		e.logf(job.ID, "info", "source footprint density: %d bytes/row (heap %d, index %d, rows %d)",
			bytesPerRow, st.HeapBytes, st.IndexBytes, st.Rows)
	}
	e.logf(job.ID, "info", "snapshot: %d candidate rows older than %s (fence %s), %d active hold(s)",
		snap.TotalRows, snap.ColdCutoff.Format(time.RFC3339), snap.Fence.Format(time.RFC3339), len(holds))
	e.progress(job)
	if snap.TotalRows == 0 {
		job.Phase = "done"
		e.logf(job.ID, "info", "nothing to archive")
		job.Status = "succeeded"
		now := time.Now().UTC()
		job.FinishedAt = &now
		return e.Meta.UpdateJob(ctx, job)
	}

	// Segment sizing: choose batches per segment from the configured target.
	batchRows := policy.BatchRows
	if batchRows <= 0 {
		batchRows = e.Cfg.Archive.BatchRows
	}
	targetRows := policy.TargetSegmentRows
	if targetRows <= 0 {
		targetRows = e.Cfg.Archive.TargetSegmentRows
	}
	blocksPerSeg := targetRows / batchRows
	if blocksPerSeg < 1 {
		blocksPerSeg = 1
	}

	// Reader goroutine: stream batches in (created_at,id) order. Batches are
	// grouped into contiguous segments; the group is then written by any free
	// worker, preserving disjoint time ranges between segments.
	batchCh := make(chan []model.Row, 8)
	readErr := make(chan error, 1)
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	go func() {
		defer close(batchCh)
		var afterTime *time.Time
		var afterID int64
		for {
			rows, lt, lid, err := e.Source.StreamBatch(readCtx, snap, afterTime, afterID, batchRows)
			if err != nil {
				readErr <- err
				return
			}
			if len(rows) == 0 {
				return
			}
			select {
			case batchCh <- rows:
			case <-readCtx.Done():
				return
			}
			afterTime, afterID = lt, lid
		}
	}()

	workers := e.Cfg.Archive.Workers
	if workers <= 0 {
		workers = runtime.NumCPU() / 2
	}
	if workers < 1 {
		workers = 1
	}

	type segResult struct {
		seg *control.Segment
		err error
	}
	groupCh := make(chan [][]model.Row, workers)
	results := make(chan segResult, workers)
	ctxWrite, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()
	var wg sync.WaitGroup
	var nextSeq int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for group := range groupCh {
				var rows []model.Row
				for _, b := range group {
					rows = append(rows, b...)
				}
				if len(rows) == 0 {
					continue
				}
				seq := atomic.AddInt64(&nextSeq, 1)
				seg, err := e.writeSegment(ctxWrite, job, policy, rows, seq, bytesPerRow)
				results <- segResult{seg: seg, err: err}
			}
		}()
	}
	go func() {
		defer close(groupCh)
		group := make([][]model.Row, 0, blocksPerSeg)
		for b := range batchCh {
			group = append(group, b)
			if len(group) >= blocksPerSeg {
				select {
				case groupCh <- group:
				case <-ctxWrite.Done():
					return
				}
				group = make([][]model.Row, 0, blocksPerSeg)
			}
		}
		if len(group) > 0 {
			select {
			case groupCh <- group:
			case <-ctxWrite.Done():
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()

	job.Phase = "archiving"
	var segs []*control.Segment
	var firstErr error
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		segs = append(segs, r.seg)
		job.ProgressRows += r.seg.Rows
		job.SegmentsDone++
		job.BytesWritten += r.seg.SizeBytes
		job.SourceBytes += r.seg.SourceBytes
		e.progress(job)
	}
	select {
	case err := <-readErr:
		if firstErr == nil {
			firstErr = err
		}
	default:
	}
	if firstErr != nil {
		return e.fail(ctx, job, firstErr)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].MinTime < segs[j].MinTime })

	// Verify every segment end-to-end before touching the source.
	job.Phase = "verifying"
	e.progress(job)
	e.logf(job.ID, "info", "verifying %d segment(s) before source eviction", len(segs))
	for _, seg := range segs {
		if err := e.verifySegment(ctx, seg); err != nil {
			return e.fail(ctx, job, fmt.Errorf("verify %s: %w", seg.Path, err))
		}
	}

	// Evict source rows, one segment at a time, skipping held rows.
	job.Phase = "evicting"
	e.logf(job.ID, "info", "evicting archived rows from source in %d-row chunks", e.Cfg.Archive.DeleteChunkRows)
	holds, err = e.Meta.Holds(ctx, true)
	if err != nil {
		return e.fail(ctx, job, err)
	}
	pause := time.Duration(e.Cfg.Archive.DeletePauseMS) * time.Millisecond
	var heldRows, evictedRows int64
	for i, seg := range segs {
		if err := ctx.Err(); err != nil {
			return e.fail(ctx, job, err)
		}
		ds, err := e.Source.EvictSegment(ctx, seg.MinTime, seg.MaxTime, seg.MinID, seg.MaxID,
			snap.ColdCutoff.UnixMilli(), snap.Fence.UnixMilli(), holds,
			e.Cfg.Archive.DeleteChunkRows, pause, nil)
		if err != nil {
			return e.fail(ctx, job, fmt.Errorf("evict segment %d: %w", seg.ID, err))
		}
		heldRows += ds.HeldMarked
		evictedRows += ds.Deleted
		seg.SourceDeletedRows = ds.Deleted + ds.HeldMarked
		if err := e.Meta.UpdateSegmentDelete(ctx, seg.ID, seg.SourceDeletedRows, seg.MaxTime); err != nil {
			return e.fail(ctx, job, err)
		}
		job.RowsDestroyed = evictedRows
		job.Detail = fmt.Sprintf("evicted %d/%d segments", i+1, len(segs))
		e.logf(job.ID, "info", "segment %d (%s): deleted %d source rows, retained %d held rows, %d chunks, max chunk %dms",
			seg.ID, filepath.Base(seg.Path), ds.Deleted, ds.HeldMarked, ds.Chunks, ds.MaxChunkMillis)
		e.progress(job)
	}
	job.RowsHeld = heldRows
	job.Status = "succeeded"
	job.Phase = "done"
	now := time.Now().UTC()
	job.FinishedAt = &now
	_ = e.Meta.SetPolicyRun(ctx, policy.ID, false)
	e.logf(job.ID, "info", "archive complete: %d segments, %d cold rows, %d source rows evicted, %d held",
		len(segs), job.ProgressRows, sumDeleted(segs), heldRows)
	return e.Meta.UpdateJob(ctx, job)
}

func sumDeleted(segs []*control.Segment) int64 {
	var n int64
	for _, s := range segs {
		n += s.SourceDeletedRows
	}
	return n
}

func (e *Engine) fail(ctx context.Context, job *control.Job, err error) error {
	job.Status = "failed"
	job.Error = err.Error()
	now := time.Now().UTC()
	job.FinishedAt = &now
	e.logf(job.ID, "error", "%v", err)
	_ = e.Meta.UpdateJob(context.Background(), job)
	return err
}

func (e *Engine) writeSegment(ctx context.Context, job *control.Job, policy *control.Policy, rows []model.Row, seq int64, bytesPerRow int64) (*control.Segment, error) {
	dir := e.segDir(policy.TableName)
	day := time.UnixMicro(rows[0][10].I).UTC().Format("2006/01/02")
	name := fmt.Sprintf("part-%06d-%d.seg", job.ID, seq)
	path := filepath.Join(dir, day, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	schema := model.Transactions
	minT, maxT := rows[0][10].I, rows[0][10].I
	minID, maxID := rows[0][0].I, rows[0][0].I
	for _, r := range rows {
		if r[10].I < minT {
			minT = r[10].I
		}
		if r[10].I > maxT {
			maxT = r[10].I
		}
		if r[0].I < minID {
			minID = r[0].I
		}
		if r[0].I > maxID {
			maxID = r[0].I
		}
	}
	meta := cold.SegmentMeta{
		Table:       policy.TableName,
		Schema:      schema,
		CreatedAt:   time.Now().UTC(),
		SourceRunID: fmt.Sprintf("job-%d", job.ID),
		MinTime:     minT,
		MaxTime:     maxT,
		Rows:        int64(len(rows)),
		AppVersion:  "1.0.0",
	}
	level := 9
	sw, err := cold.CreateSegment(path, meta, level)
	if err != nil {
		return nil, err
	}
	for off := 0; off < len(rows); off += 8192 {
		end := off + 8192
		if end > len(rows) {
			end = len(rows)
		}
		if err := sw.AppendBlock(rows[off:end], schema); err != nil {
			sw.Abort()
			return nil, err
		}
	}
	foot, err := sw.Close()
	if err != nil {
		return nil, err
	}
	st, _ := os.Stat(path)
	srcBytes := int64(len(rows)) * bytesPerRow
	// Content hash over (id, txn_no, created_at) from what was durably written.
	h := sha256.New()
	for _, r := range rows {
		fmt.Fprintf(h, "%d|%s|%d\n", r[0].I, r[1].S, r[10].I)
	}
	seg := &control.Segment{
		JobID:       job.ID,
		PolicyID:    policy.ID,
		TableName:   policy.TableName,
		Path:        path,
		SHA256:      foot.SHA256,
		ContentHash: hex.EncodeToString(h.Sum(nil)),
		SizeBytes:   st.Size(),
		RawBytes:    footRawBytes(foot),
		SourceBytes: srcBytes,
		Rows:        int64(len(rows)),
		MinTime:     minT,
		MaxTime:     maxT,
		MinID:       minID,
		MaxID:       maxID,
		Status:      "active",
	}
	// Structural post-write check: header/footer, block index, SHA256 and a
	// sample decode of the first and last block. The full streaming decode is
	// performed in the verify phase before any source row is deleted.
	if err := e.verifyStructure(seg); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("post-write verification: %w", err)
	}
	if err := e.Meta.CreateSegment(ctx, seg); err != nil {
		os.Remove(path)
		return nil, err
	}
	if e.Hooks.OnVerified != nil {
		e.Hooks.OnVerified(seg)
	}
	e.logf(job.ID, "info", "wrote segment %s: %d rows, %s -> %s (%.1fx), raw %s",
		filepath.Base(path), seg.Rows, humanBytes(seg.SourceBytes), humanBytes(seg.SizeBytes),
		ratio(seg.SourceBytes, seg.SizeBytes), humanBytes(seg.RawBytes))
	return seg, nil
}

func footRawBytes(f cold.Footer) int64 {
	var n int64
	for _, b := range f.Blocks {
		n += b.RawLen
	}
	return n
}

// verifyStructure validates the segment envelope and decodes the first and last
// block. It is cheap and catches truncation/header errors immediately.
func (e *Engine) verifyStructure(seg *control.Segment) error {
	r, err := cold.OpenSegment(seg.Path)
	if err != nil {
		return err
	}
	if int64(r.Rows()) != seg.Rows {
		return fmt.Errorf("segment row mismatch: meta %d, header %d", seg.Rows, r.Rows())
	}
	sum, err := cold.FileSHA256(seg.Path)
	if err != nil {
		return err
	}
	if sum != seg.SHA256 {
		return fmt.Errorf("segment sha256 mismatch")
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()
	blocks := r.Blocks()
	if len(blocks) == 0 {
		return fmt.Errorf("segment has no blocks")
	}
	for _, i := range []int{0, len(blocks) - 1} {
		blk, err := r.ReadBlock(i, dec)
		if err != nil {
			return err
		}
		if blk.Rows != blocks[i].Rows {
			return fmt.Errorf("block %d row count mismatch", i)
		}
	}
	return nil
}

// verifySegment re-reads and fully decodes the segment, then recomputes SHA256.
func (e *Engine) verifySegment(ctx context.Context, seg *control.Segment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := cold.OpenSegment(seg.Path)
	if err != nil {
		return err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()
	rows, _, err := r.VerifyStreaming(dec)
	if err != nil {
		return err
	}
	if rows != seg.Rows {
		return fmt.Errorf("segment row mismatch: meta %d, file %d", seg.Rows, rows)
	}
	sum, err := cold.FileSHA256(seg.Path)
	if err != nil {
		return err
	}
	if sum != seg.SHA256 {
		return fmt.Errorf("segment sha256 mismatch")
	}
	return nil
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// VerifyJob re-verifies every segment of a job.
func (e *Engine) VerifyJob(ctx context.Context, jobID int64) error {
	segs, err := e.Meta.Segments(ctx, true)
	if err != nil {
		return err
	}
	for i := range segs {
		if segs[i].JobID != jobID {
			continue
		}
		if err := e.verifySegment(ctx, &segs[i]); err != nil {
			return err
		}
	}
	return nil
}

// RecoverInterrupted marks jobs left running by a crash as failed.
func (e *Engine) RecoverInterrupted(ctx context.Context) error {
	jobs, err := e.Meta.Jobs(ctx, 1000)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Status == "running" || j.Status == "queued" {
			_ = e.Meta.FinishJob(ctx, j.ID, "failed", "interrupted by server restart")
		}
	}
	return nil
}
