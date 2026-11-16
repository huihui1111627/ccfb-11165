// Package destruction implements retention destruction with legal-hold
// enforcement and auditable manifests.
package destruction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
	"payment-archive/internal/cold"
	"payment-archive/internal/config"
	"payment-archive/internal/control"
	"payment-archive/internal/model"
	"payment-archive/internal/source"
)

type Engine struct {
	Cfg              config.Config
	Source           *source.Source
	Meta             *control.Store
	Log              func(jobID int64, level, msg string)
	Progress         func(j *control.Job)
	OnSegmentChanged func(path string)
}

func (e *Engine) logf(jobID int64, level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if e.Log != nil {
		e.Log(jobID, level, msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.Meta.AddLog(ctx, jobID, level, msg)
}

func (e *Engine) progress(j *control.Job) {
	if e.Progress != nil {
		e.Progress(j)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.Meta.UpdateJob(ctx, j)
}

// ManifestFile is the JSON document written to disk for every destruction run.
type ManifestFile struct {
	ManifestID    int64                   `json:"manifest_id"`
	JobID         int64                   `json:"job_id"`
	PolicyID      int64                   `json:"policy_id"`
	Table         string                  `json:"table"`
	GeneratedAt   time.Time               `json:"generated_at"`
	DestroyCutoff time.Time               `json:"destroy_cutoff"`
	RegulatoryMin int                     `json:"regulatory_min_retention_days"`
	Summary       ManifestSummary         `json:"summary"`
	Segments      []ManifestSegmentRecord `json:"segments"`
	Holds         []model.Hold            `json:"active_holds"`
	RowIDDigest   string                  `json:"row_id_digest_sha256"`
	Notes         string                  `json:"notes"`
}

type ManifestSummary struct {
	RowsDestroyed int64 `json:"rows_destroyed"`
	RowsRetained  int64 `json:"rows_retained_by_hold"`
	BytesFreed    int64 `json:"bytes_freed"`
	Segments      int   `json:"segments"`
}

type ManifestSegmentRecord struct {
	SegmentID     int64  `json:"segment_id"`
	Path          string `json:"path"`
	RowsTotal     int64  `json:"rows_total"`
	RowsDestroyed int64  `json:"rows_destroyed"`
	RowsRetained  int64  `json:"rows_retained"`
	BytesFreed    int64  `json:"bytes_freed"`
	Result        string `json:"result"`
	// OldSHA256/NewPath/NewSHA256 identify a rewritten segment that still
	// contains held rows.
	OldSHA256 string `json:"old_sha256,omitempty"`
	NewPath   string `json:"new_path,omitempty"`
	NewSHA256 string `json:"new_sha256,omitempty"`
}

// Run executes a destroy job: segments whose MaxTime is older than the
// retention cutoff are rewritten without destructible rows, then deleted.
func (e *Engine) Run(ctx context.Context, job *control.Job, policy *control.Policy) error {
	if err := e.Meta.StartJob(ctx, job.ID); err != nil {
		return err
	}
	job.Status, job.Phase = "running", "planning"
	e.progress(job)

	minDays := e.Cfg.RegulatoryMinRetentionDay
	if policy.DestroyAfterDays < minDays {
		err := fmt.Errorf("policy retention of %d days is below the regulatory minimum of %d days", policy.DestroyAfterDays, minDays)
		job.Status, job.Error = "failed", err.Error()
		now := time.Now().UTC()
		job.FinishedAt = &now
		e.logf(job.ID, "error", "%v", err)
		_ = e.Meta.UpdateJob(context.Background(), job)
		return err
	}

	holds, err := e.Meta.Holds(ctx, true)
	if err != nil {
		return e.fail(job, err)
	}
	holdSet := model.HoldSet(holds)
	cutoff := time.Now().UTC().AddDate(0, 0, -policy.DestroyAfterDays)
	segs, err := e.Meta.ActiveSegments(ctx, policy.TableName)
	if err != nil {
		return e.fail(job, err)
	}
	var targets []control.Segment
	for _, seg := range segs {
		if seg.MaxTime < cutoff.UnixMicro() {
			targets = append(targets, seg)
		}
	}
	job.TotalRows = 0
	for _, seg := range targets {
		job.TotalRows += seg.Rows
	}
	e.logf(job.ID, "info", "destroy plan: %d segment(s), %d rows with max_time older than %s, %d active hold(s)",
		len(targets), job.TotalRows, cutoff.Format(time.RFC3339), len(holds))
	job.Phase = "rewriting"
	e.progress(job)

	var (
		records    []ManifestSegmentRecord
		destroyed  int64
		retained   int64
		bytesFreed int64
		idDigest   = sha256.New()
	)
	for i := range targets {
		if err := ctx.Err(); err != nil {
			return e.fail(job, err)
		}
		seg := &targets[i]
		rec, err := e.processSegment(ctx, job, policy, seg, cutoff, holdSet, idDigest)
		if err != nil {
			return e.fail(job, err)
		}
		records = append(records, *rec)
		destroyed += rec.RowsDestroyed
		retained += rec.RowsRetained
		bytesFreed += rec.BytesFreed
		job.RowsDestroyed = destroyed
		job.RowsHeld = retained
		job.ProgressRows += rec.RowsTotal
		job.SegmentsDone++
		job.Detail = fmt.Sprintf("processed %d/%d segments", i+1, len(targets))
		e.progress(job)
	}

	// Remove source rows that were previously marked archived and are now
	// expired. Rows under hold are preserved.
	job.Phase = "purging_source"
	e.progress(job)
	purged, err := e.Source.DeleteExpiredArchived(ctx, cutoff, holds, e.Cfg.Archive.DeleteChunkRows,
		time.Duration(e.Cfg.Archive.DeletePauseMS)*time.Millisecond)
	if err != nil {
		return e.fail(job, err)
	}
	e.logf(job.ID, "info", "purged %d previously-archived source rows past retention", purged)

	// Write manifest.
	job.Phase = "manifest"
	mf := ManifestFile{
		JobID:         job.ID,
		PolicyID:      policy.ID,
		Table:         policy.TableName,
		GeneratedAt:   time.Now().UTC(),
		DestroyCutoff: cutoff,
		RegulatoryMin: minDays,
		Summary: ManifestSummary{
			RowsDestroyed: destroyed,
			RowsRetained:  retained,
			BytesFreed:    bytesFreed,
			Segments:      len(targets),
		},
		Segments:    records,
		Holds:       holds,
		RowIDDigest: hex.EncodeToString(idDigest.Sum(nil)),
		Notes:       "Cold segments were rewritten without expired, non-held rows and then unlinked. Rows under an active legal hold were retained and are listed in the retained count. The row_id_digest_sha256 is the SHA-256 of the newline-separated destroyed row IDs in ascending order.",
	}
	dir := filepath.Join(e.Cfg.DataDir, "manifests")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return e.fail(job, err)
	}
	fileName := fmt.Sprintf("destroy-%06d-%s.json", job.ID, time.Now().UTC().Format("20060102T150405Z"))
	filePath := filepath.Join(dir, fileName)
	doc := struct {
		*ManifestFile
		ManifestID int64 `json:"manifest_id"`
	}{ManifestFile: &mf}
	doc.ManifestID = 0
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return e.fail(job, err)
	}
	fileHash := sha256.Sum256(b)
	if err := os.WriteFile(filePath, b, 0o644); err != nil {
		return e.fail(job, err)
	}
	rec := &control.Manifest{
		JobID:         job.ID,
		PolicyID:      policy.ID,
		TableName:     policy.TableName,
		CreatedAt:     time.Now().UTC(),
		Cutoff:        cutoff,
		RowsDestroyed: destroyed,
		RowsHeld:      retained,
		BytesFreed:    bytesFreed,
		Segments:      len(targets),
		IDSHA256:      mf.RowIDDigest,
		FilePath:      filePath,
		FileSHA256:    hex.EncodeToString(fileHash[:]),
		FileBytes:     int64(len(b)),
		Summary: fmt.Sprintf("destroyed %d rows across %d segment(s); retained %d held rows",
			destroyed, len(targets), retained),
	}
	if err := e.Meta.CreateManifest(ctx, rec); err != nil {
		return e.fail(job, err)
	}
	for i := range records {
		r := &records[i]
		_ = e.Meta.AddManifestSegment(ctx, &control.ManifestSegment{
			ManifestID: rec.ID, SegmentID: r.SegmentID, Path: r.Path,
			RowsTotal: r.RowsTotal, RowsDestroyed: r.RowsDestroyed,
			RowsRetained: r.RowsRetained, BytesFreed: r.BytesFreed, Result: r.Result,
		})
	}
	// Rewrite the manifest file with its own id for traceability.
	mf.ManifestID = rec.ID
	b2, _ := json.MarshalIndent(mf, "", "  ")
	fileHash2 := sha256.Sum256(b2)
	if err := os.WriteFile(filePath, b2, 0o644); err == nil {
		_, _ = e.Meta.DB().ExecContext(ctx, `UPDATE manifests SET file_sha256=?, file_bytes=? WHERE id=?`,
			hex.EncodeToString(fileHash2[:]), len(b2), rec.ID)
	}

	job.Status, job.Phase = "succeeded", "done"
	now := time.Now().UTC()
	job.FinishedAt = &now
	_ = e.Meta.SetPolicyRun(ctx, policy.ID, true)
	e.logf(job.ID, "info", "destroy complete: %d rows destroyed, %d held rows retained, %d bytes freed, manifest %s",
		destroyed, retained, bytesFreed, filePath)
	return e.Meta.UpdateJob(ctx, job)
}

func (e *Engine) fail(job *control.Job, err error) error {
	job.Status, job.Error = "failed", err.Error()
	now := time.Now().UTC()
	job.FinishedAt = &now
	e.logf(job.ID, "error", "%v", err)
	_ = e.Meta.UpdateJob(context.Background(), job)
	return err
}

func (e *Engine) processSegment(ctx context.Context, job *control.Job, policy *control.Policy,
	seg *control.Segment, cutoff time.Time, holds model.HoldSet, idDigest interface{ Write([]byte) (int, error) }) (*ManifestSegmentRecord, error) {
	rec := &ManifestSegmentRecord{
		SegmentID: seg.ID,
		Path:      seg.Path,
		RowsTotal: seg.Rows,
		OldSHA256: seg.SHA256,
	}
	r, err := cold.OpenSegment(seg.Path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", seg.Path, err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	schema := r.Meta().Schema
	cutoffUS := cutoff.UnixMicro()
	var keep []model.Row
	var destroyedIDs []int64
	pgRead := 0
	for bi := range r.Blocks() {
		blk, err := r.ReadBlock(bi, dec)
		if err != nil {
			return nil, fmt.Errorf("read %s block %d: %w", seg.Path, bi, err)
		}
		pgRead += blk.Rows
		for i := 0; i < blk.Rows; i++ {
			row := make(model.Row, len(schema.Columns))
			for ci, col := range schema.Columns {
				dc := &blk.Cols[ci]
				null := dc.Null != nil && dc.Null[i]
				if col.Kind == model.KindString {
					row[ci] = model.Value{S: dc.Strs[i], Null: null}
				} else {
					row[ci] = model.Value{I: dc.Ints[i], Null: null}
				}
			}
			expired := row[10].I < cutoffUS
			if expired && !holds.Matches(row) {
				destroyedIDs = append(destroyedIDs, row[0].I)
				rec.RowsDestroyed++
				continue
			}
			keep = append(keep, row)
			if expired {
				rec.RowsRetained++
			}
		}
	}
	if int64(pgRead) != seg.Rows {
		return nil, fmt.Errorf("segment %d row count %d != meta %d", seg.ID, pgRead, seg.Rows)
	}
	sort.Slice(destroyedIDs, func(i, j int) bool { return destroyedIDs[i] < destroyedIDs[j] })
	for _, id := range destroyedIDs {
		fmt.Fprintf(idDigest, "%d\n", id)
	}
	rec.BytesFreed = seg.SizeBytes
	if len(keep) == 0 {
		// Whole segment destroyed.
		if err := os.Remove(seg.Path); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err := e.Meta.MarkSegmentDestroyed(ctx, seg.ID); err != nil {
			return nil, err
		}
		if e.OnSegmentChanged != nil {
			e.OnSegmentChanged(seg.Path)
		}
		rec.Result = "destroyed"
		e.logf(job.ID, "info", "segment %d destroyed entirely (%d rows, %d bytes)", seg.ID, rec.RowsDestroyed, seg.SizeBytes)
		return rec, nil
	}

	// Partial: rewrite a new segment with the retained rows.
	dir := filepath.Dir(seg.Path)
	newName := fmt.Sprintf("retained-seg%06d-%d.seg", seg.ID, time.Now().UTC().UnixNano())
	newPath := filepath.Join(dir, newName)
	minT, maxT := keep[0][10].I, keep[0][10].I
	minID, maxID := keep[0][0].I, keep[0][0].I
	for _, row := range keep {
		if row[10].I < minT {
			minT = row[10].I
		}
		if row[10].I > maxT {
			maxT = row[10].I
		}
		if row[0].I < minID {
			minID = row[0].I
		}
		if row[0].I > maxID {
			maxID = row[0].I
		}
	}
	meta := cold.SegmentMeta{
		Table:            seg.TableName,
		Schema:           schema,
		CreatedAt:        time.Now().UTC(),
		SourceRunID:      fmt.Sprintf("destroy-job-%d", job.ID),
		MinTime:          minT,
		MaxTime:          maxT,
		Rows:             int64(len(keep)),
		CompressionLevel: 9,
		AppVersion:       "1.0.0",
	}
	sw, err := cold.CreateSegment(newPath, meta, 9)
	if err != nil {
		return nil, err
	}
	for off := 0; off < len(keep); off += 8192 {
		end := off + 8192
		if end > len(keep) {
			end = len(keep)
		}
		if err := sw.AppendBlock(keep[off:end], schema); err != nil {
			sw.Abort()
			return nil, err
		}
	}
	foot, err := sw.Close()
	if err != nil {
		return nil, err
	}
	st, _ := os.Stat(newPath)
	// Verify new segment before deleting the old one.
	nr, err := cold.OpenSegment(newPath)
	if err != nil {
		return nil, err
	}
	rowsRead, _, err := nr.VerifyStreaming(dec)
	if err != nil || rowsRead != int64(len(keep)) {
		return nil, fmt.Errorf("retained segment verification failed: %v", err)
	}
	// Content hash describes the retained payload, not the original segment.
	ch := sha256.New()
	for _, row := range keep {
		fmt.Fprintf(ch, "%d|%s|%d\n", row[0].I, row[1].S, row[10].I)
	}
	newSeg := &control.Segment{
		JobID:       job.ID,
		PolicyID:    policy.ID,
		TableName:   seg.TableName,
		Path:        newPath,
		SHA256:      foot.SHA256,
		ContentHash: hex.EncodeToString(ch.Sum(nil)),
		SizeBytes:   st.Size(),
		RawBytes:    seg.RawBytes * int64(len(keep)) / max64(seg.Rows, 1),
		SourceBytes: seg.SourceBytes * int64(len(keep)) / max64(seg.Rows, 1),
		Rows:        int64(len(keep)),
		MinTime:     minT,
		MaxTime:     maxT,
		MinID:       minID,
		MaxID:       maxID,
		Status:      "active",
	}
	if err := e.Meta.CreateSegment(ctx, newSeg); err != nil {
		os.Remove(newPath)
		return nil, err
	}
	if err := os.Remove(seg.Path); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := e.Meta.MarkSegmentRewritten(ctx, seg.ID); err != nil {
		return nil, err
	}
	if e.OnSegmentChanged != nil {
		e.OnSegmentChanged(seg.Path)
	}
	rec.RowsTotal = seg.Rows
	rec.BytesFreed = seg.SizeBytes - st.Size()
	rec.Result = "rewritten"
	rec.NewPath = newPath
	rec.NewSHA256 = foot.SHA256
	e.logf(job.ID, "info", "segment %d rewritten: destroyed %d, retained %d held rows -> %s",
		seg.ID, rec.RowsDestroyed, rec.RowsRetained, filepath.Base(newPath))
	return rec, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
