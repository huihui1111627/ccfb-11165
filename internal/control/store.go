// Package control stores platform metadata (policies, jobs, segments, legal
// holds and destruction manifests) in an embedded SQLite database.
package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
	"payment-archive/internal/model"
)

type Store struct {
	db *sql.DB
}

type Policy struct {
	ID                int64      `json:"id"`
	TableName         string     `json:"table_name"`
	TimeField         string     `json:"time_field"`
	PKField           string     `json:"pk_field"`
	ColdAfterDays     int        `json:"cold_after_days"`
	DestroyAfterDays  int        `json:"destroy_after_days"`
	BatchRows         int        `json:"batch_rows"`
	TargetSegmentRows int        `json:"target_segment_rows"`
	Enabled           bool       `json:"enabled"`
	Description       string     `json:"description"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	LastRunAt         *time.Time `json:"last_run_at,omitempty"`
	LastDestroyAt     *time.Time `json:"last_destroy_at,omitempty"`
}

type Job struct {
	ID            int64      `json:"id"`
	Type          string     `json:"type"`
	PolicyID      int64      `json:"policy_id"`
	Status        string     `json:"status"` // queued, running, succeeded, failed, cancelled
	Phase         string     `json:"phase"`
	ProgressRows  int64      `json:"progress_rows"`
	TotalRows     int64      `json:"total_rows"`
	SegmentsDone  int        `json:"segments_done"`
	BytesWritten  int64      `json:"bytes_written"`
	SourceBytes   int64      `json:"source_bytes"`
	RowsDestroyed int64      `json:"rows_destroyed"`
	RowsHeld      int64      `json:"rows_held"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	Error         string     `json:"error,omitempty"`
	Detail        string     `json:"detail,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
}

type Segment struct {
	ID                int64      `json:"id"`
	JobID             int64      `json:"job_id"`
	PolicyID          int64      `json:"policy_id"`
	TableName         string     `json:"table_name"`
	Path              string     `json:"path"`
	SHA256            string     `json:"sha256"`
	ContentHash       string     `json:"content_hash"`
	SizeBytes         int64      `json:"size_bytes"`
	RawBytes          int64      `json:"raw_bytes"`
	SourceBytes       int64      `json:"source_bytes"`
	Rows              int64      `json:"rows"`
	MinTime           int64      `json:"min_time"`
	MaxTime           int64      `json:"max_time"`
	MinID             int64      `json:"min_id"`
	MaxID             int64      `json:"max_id"`
	Status            string     `json:"status"` // active, rewritten, destroyed
	SourceDeletedRows int64      `json:"source_deleted_rows"`
	DeleteWatermark   int64      `json:"delete_watermark"`
	CreatedAt         time.Time  `json:"created_at"`
	VerifiedAt        *time.Time `json:"verified_at,omitempty"`
	DestroyedAt       *time.Time `json:"destroyed_at,omitempty"`
}

type Hold = model.Hold

type Manifest struct {
	ID            int64     `json:"id"`
	JobID         int64     `json:"job_id"`
	PolicyID      int64     `json:"policy_id"`
	TableName     string    `json:"table_name"`
	CreatedAt     time.Time `json:"created_at"`
	Cutoff        time.Time `json:"cutoff"`
	RowsDestroyed int64     `json:"rows_destroyed"`
	RowsHeld      int64     `json:"rows_held"`
	BytesFreed    int64     `json:"bytes_freed"`
	Segments      int       `json:"segments"`
	IDSHA256      string    `json:"id_sha256"`
	FilePath      string    `json:"file_path"`
	FileSHA256    string    `json:"file_sha256"`
	FileBytes     int64     `json:"file_bytes"`
	Summary       string    `json:"summary"`
}

type ManifestSegment struct {
	ManifestID    int64  `json:"manifest_id"`
	SegmentID     int64  `json:"segment_id"`
	Path          string `json:"path"`
	RowsTotal     int64  `json:"rows_total"`
	RowsDestroyed int64  `json:"rows_destroyed"`
	RowsRetained  int64  `json:"rows_retained"`
	BytesFreed    int64  `json:"bytes_freed"`
	Result        string `json:"result"`
}

type JobLog struct {
	ID      int64     `json:"id"`
	JobID   int64     `json:"job_id"`
	TS      time.Time `json:"ts"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// DB exposes the underlying handle for small internal maintenance queries.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS policies (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			table_name TEXT NOT NULL,
			time_field TEXT NOT NULL,
			pk_field TEXT NOT NULL,
			cold_after_days INTEGER NOT NULL,
			destroy_after_days INTEGER NOT NULL,
			batch_rows INTEGER NOT NULL DEFAULT 50000,
			target_segment_rows INTEGER NOT NULL DEFAULT 1000000,
			enabled INTEGER NOT NULL DEFAULT 1,
			description TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			last_run_at INTEGER,
			last_destroy_at INTEGER,
			UNIQUE(table_name)
		)`,
		`CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			policy_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			phase TEXT NOT NULL DEFAULT '',
			progress_rows INTEGER NOT NULL DEFAULT 0,
			total_rows INTEGER NOT NULL DEFAULT 0,
			segments_done INTEGER NOT NULL DEFAULT 0,
			bytes_written INTEGER NOT NULL DEFAULT 0,
			source_bytes INTEGER NOT NULL DEFAULT 0,
			rows_destroyed INTEGER NOT NULL DEFAULT 0,
			rows_held INTEGER NOT NULL DEFAULT 0,
			started_at INTEGER,
			finished_at INTEGER,
			error TEXT NOT NULL DEFAULT '',
			detail TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS job_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id INTEGER NOT NULL,
			ts INTEGER NOT NULL,
			level TEXT NOT NULL,
			message TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_job_logs_job ON job_logs(job_id, id)`,
		`CREATE TABLE IF NOT EXISTS segments (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id INTEGER NOT NULL,
			policy_id INTEGER NOT NULL,
			table_name TEXT NOT NULL,
			path TEXT NOT NULL UNIQUE,
			sha256 TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			size_bytes INTEGER NOT NULL,
			raw_bytes INTEGER NOT NULL,
			source_bytes INTEGER NOT NULL,
			rows INTEGER NOT NULL,
			min_time INTEGER NOT NULL,
			max_time INTEGER NOT NULL,
			min_id INTEGER NOT NULL,
			max_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			source_deleted_rows INTEGER NOT NULL DEFAULT 0,
			delete_watermark INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			verified_at INTEGER,
			destroyed_at INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS idx_segments_active ON segments(status, table_name, max_time DESC)`,
		`CREATE TABLE IF NOT EXISTS holds (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			case_no TEXT NOT NULL,
			reason TEXT NOT NULL,
			scope TEXT NOT NULL,
			target_value TEXT NOT NULL DEFAULT '',
			target_from INTEGER NOT NULL DEFAULT 0,
			target_to INTEGER NOT NULL DEFAULT 0,
			time_from INTEGER,
			time_to INTEGER,
			active INTEGER NOT NULL DEFAULT 1,
			created_by TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			released_at INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS manifests (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id INTEGER NOT NULL,
			policy_id INTEGER NOT NULL,
			table_name TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			cutoff INTEGER NOT NULL,
			rows_destroyed INTEGER NOT NULL,
			rows_held INTEGER NOT NULL,
			bytes_freed INTEGER NOT NULL,
			segments INTEGER NOT NULL,
			id_sha256 TEXT NOT NULL,
			file_path TEXT NOT NULL,
			file_sha256 TEXT NOT NULL,
			file_bytes INTEGER NOT NULL,
			summary TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS manifest_segments (
			manifest_id INTEGER NOT NULL,
			segment_id INTEGER NOT NULL,
			path TEXT NOT NULL,
			rows_total INTEGER NOT NULL,
			rows_destroyed INTEGER NOT NULL,
			rows_retained INTEGER NOT NULL,
			bytes_freed INTEGER NOT NULL,
			result TEXT NOT NULL,
			PRIMARY KEY(manifest_id, segment_id)
		)`,
		`CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("migrate: %w\n%s", err, q)
		}
	}
	return nil
}

func ms(t time.Time) int64 { return t.UnixMilli() }
func msPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UnixMilli()
}
func fromMS(v int64) time.Time { return time.UnixMilli(v).UTC() }
func fromMSPtr(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := fromMS(v.Int64)
	return &t
}

// ---------------- Policies ----------------

func (s *Store) CreatePolicy(ctx context.Context, p *Policy) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `INSERT INTO policies
		(table_name,time_field,pk_field,cold_after_days,destroy_after_days,batch_rows,target_segment_rows,enabled,description,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		p.TableName, p.TimeField, p.PKField, p.ColdAfterDays, p.DestroyAfterDays, p.BatchRows, p.TargetSegmentRows, boolInt(p.Enabled), p.Description, ms(now), ms(now))
	if err != nil {
		return err
	}
	id, _ := res.LastInsertId()
	p.ID = id
	p.CreatedAt, p.UpdatedAt = now, now
	return nil
}

func (s *Store) UpdatePolicy(ctx context.Context, p *Policy) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE policies SET
		cold_after_days=?, destroy_after_days=?, batch_rows=?, target_segment_rows=?, enabled=?, description=?, updated_at=? WHERE id=?`,
		p.ColdAfterDays, p.DestroyAfterDays, p.BatchRows, p.TargetSegmentRows, boolInt(p.Enabled), p.Description, ms(now), p.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("policy %d not found", p.ID)
	}
	p.UpdatedAt = now
	return nil
}

func (s *Store) SetPolicyRun(ctx context.Context, id int64, destroy bool) error {
	col := "last_run_at"
	if destroy {
		col = "last_destroy_at"
	}
	_, err := s.db.ExecContext(ctx, `UPDATE policies SET `+col+`=? WHERE id=?`, ms(time.Now().UTC()), id)
	return err
}

func (s *Store) Policies(ctx context.Context) ([]Policy, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,table_name,time_field,pk_field,cold_after_days,destroy_after_days,batch_rows,target_segment_rows,enabled,description,created_at,updated_at,last_run_at,last_destroy_at FROM policies ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Policy
	for rows.Next() {
		var p Policy
		var created, updated int64
		var lastRun, lastDestroy sql.NullInt64
		var enabled int
		if err := rows.Scan(&p.ID, &p.TableName, &p.TimeField, &p.PKField, &p.ColdAfterDays, &p.DestroyAfterDays, &p.BatchRows, &p.TargetSegmentRows, &enabled, &p.Description, &created, &updated, &lastRun, &lastDestroy); err != nil {
			return nil, err
		}
		p.Enabled = enabled != 0
		p.CreatedAt, p.UpdatedAt = fromMS(created), fromMS(updated)
		p.LastRunAt, p.LastDestroyAt = fromMSPtr(lastRun), fromMSPtr(lastDestroy)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) Policy(ctx context.Context, id int64) (*Policy, error) {
	all, err := s.Policies(ctx)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].ID == id {
			return &all[i], nil
		}
	}
	return nil, fmt.Errorf("policy %d not found", id)
}

// ---------------- Jobs ----------------

func (s *Store) CreateJob(ctx context.Context, typ string, policyID int64) (*Job, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `INSERT INTO jobs(type,policy_id,status,phase,created_at) VALUES(?,?,?,?,?)`,
		typ, policyID, "queued", "queued", ms(now))
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Job{ID: id, Type: typ, PolicyID: policyID, Status: "queued", Phase: "queued", CreatedAt: now}, nil
}

func (s *Store) StartJob(ctx context.Context, id int64) error {
	now := ms(time.Now().UTC())
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='running', phase='starting', started_at=? WHERE id=?`, now, id)
	return err
}

func (s *Store) UpdateJob(ctx context.Context, j *Job) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?, phase=?, progress_rows=?, total_rows=?, segments_done=?, bytes_written=?, source_bytes=?, rows_destroyed=?, rows_held=?, error=?, detail=?, finished_at=? WHERE id=?`,
		j.Status, j.Phase, j.ProgressRows, j.TotalRows, j.SegmentsDone, j.BytesWritten, j.SourceBytes, j.RowsDestroyed, j.RowsHeld, j.Error, j.Detail, msPtr(j.FinishedAt), j.ID)
	return err
}

func (s *Store) FinishJob(ctx context.Context, id int64, status, errMsg string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?, phase=?, finished_at=?, error=? WHERE id=?`, status, status, ms(now), errMsg, id)
	return err
}

func (s *Store) Jobs(ctx context.Context, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,type,policy_id,status,phase,progress_rows,total_rows,segments_done,bytes_written,source_bytes,rows_destroyed,rows_held,started_at,finished_at,error,detail,created_at FROM jobs ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		var created int64
		var started, finished sql.NullInt64
		if err := rows.Scan(&j.ID, &j.Type, &j.PolicyID, &j.Status, &j.Phase, &j.ProgressRows, &j.TotalRows, &j.SegmentsDone, &j.BytesWritten, &j.SourceBytes, &j.RowsDestroyed, &j.RowsHeld, &started, &finished, &j.Error, &j.Detail, &created); err != nil {
			return nil, err
		}
		j.StartedAt, j.FinishedAt = fromMSPtr(started), fromMSPtr(finished)
		j.CreatedAt = fromMS(created)
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) Job(ctx context.Context, id int64) (*Job, error) {
	all, err := s.Jobs(ctx, 1_000_000)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].ID == id {
			return &all[i], nil
		}
	}
	return nil, fmt.Errorf("job %d not found", id)
}

func (s *Store) RunningJob(ctx context.Context, typ string, policyID int64) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE type=? AND policy_id=? AND status IN ('queued','running')`, typ, policyID).Scan(&n)
	return n > 0, err
}

func (s *Store) AddLog(ctx context.Context, jobID int64, level, msg string) {
	_, _ = s.db.ExecContext(ctx, `INSERT INTO job_logs(job_id,ts,level,message) VALUES(?,?,?,?)`, jobID, ms(time.Now().UTC()), level, msg)
}

func (s *Store) JobLogs(ctx context.Context, jobID int64, limit int) ([]JobLog, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,job_id,ts,level,message FROM job_logs WHERE job_id=? ORDER BY id DESC LIMIT ?`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobLog
	for rows.Next() {
		var l JobLog
		var ts int64
		if err := rows.Scan(&l.ID, &l.JobID, &ts, &l.Level, &l.Message); err != nil {
			return nil, err
		}
		l.TS = fromMS(ts)
		out = append(out, l)
	}
	return out, rows.Err()
}

// ---------------- Segments ----------------

func (s *Store) CreateSegment(ctx context.Context, seg *Segment) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `INSERT INTO segments
		(job_id,policy_id,table_name,path,sha256,content_hash,size_bytes,raw_bytes,source_bytes,rows,min_time,max_time,min_id,max_id,status,source_deleted_rows,delete_watermark,created_at,verified_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		seg.JobID, seg.PolicyID, seg.TableName, seg.Path, seg.SHA256, seg.ContentHash, seg.SizeBytes, seg.RawBytes, seg.SourceBytes,
		seg.Rows, seg.MinTime, seg.MaxTime, seg.MinID, seg.MaxID, seg.Status, seg.SourceDeletedRows, seg.DeleteWatermark, ms(now), ms(now))
	if err != nil {
		return err
	}
	id, _ := res.LastInsertId()
	seg.ID = id
	seg.CreatedAt = now
	t := now
	seg.VerifiedAt = &t
	return nil
}

func (s *Store) UpdateSegmentDelete(ctx context.Context, id int64, deletedRows, watermark int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE segments SET source_deleted_rows=?, delete_watermark=? WHERE id=?`, deletedRows, watermark, id)
	return err
}

func (s *Store) MarkSegmentRewritten(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE segments SET status='rewritten', destroyed_at=? WHERE id=?`, ms(time.Now().UTC()), id)
	return err
}

func (s *Store) MarkSegmentDestroyed(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE segments SET status='destroyed', destroyed_at=? WHERE id=?`, ms(time.Now().UTC()), id)
	return err
}

func (s *Store) Segments(ctx context.Context, includeInactive bool) ([]Segment, error) {
	q := `SELECT id,job_id,policy_id,table_name,path,sha256,content_hash,size_bytes,raw_bytes,source_bytes,rows,min_time,max_time,min_id,max_id,status,source_deleted_rows,delete_watermark,created_at,verified_at,destroyed_at FROM segments`
	if !includeInactive {
		q += ` WHERE status='active'`
	}
	q += ` ORDER BY min_time ASC, id ASC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Segment
	for rows.Next() {
		var seg Segment
		var created int64
		var verified, destroyed sql.NullInt64
		if err := rows.Scan(&seg.ID, &seg.JobID, &seg.PolicyID, &seg.TableName, &seg.Path, &seg.SHA256, &seg.ContentHash, &seg.SizeBytes, &seg.RawBytes, &seg.SourceBytes, &seg.Rows, &seg.MinTime, &seg.MaxTime, &seg.MinID, &seg.MaxID, &seg.Status, &seg.SourceDeletedRows, &seg.DeleteWatermark, &created, &verified, &destroyed); err != nil {
			return nil, err
		}
		seg.CreatedAt = fromMS(created)
		seg.VerifiedAt, seg.DestroyedAt = fromMSPtr(verified), fromMSPtr(destroyed)
		out = append(out, seg)
	}
	return out, rows.Err()
}

func (s *Store) ActiveSegments(ctx context.Context, table string) ([]Segment, error) {
	all, err := s.Segments(ctx, false)
	if err != nil {
		return nil, err
	}
	if table == "" {
		return all, nil
	}
	var out []Segment
	for _, seg := range all {
		if seg.TableName == table {
			out = append(out, seg)
		}
	}
	return out, nil
}

func (s *Store) PendingDeleteSegments(ctx context.Context) ([]Segment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,job_id,policy_id,table_name,path,sha256,content_hash,size_bytes,raw_bytes,source_bytes,rows,min_time,max_time,min_id,max_id,status,source_deleted_rows,delete_watermark,created_at,verified_at,destroyed_at FROM segments WHERE status='active' AND source_deleted_rows < rows ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Segment
	for rows.Next() {
		var seg Segment
		var created int64
		var verified, destroyed sql.NullInt64
		if err := rows.Scan(&seg.ID, &seg.JobID, &seg.PolicyID, &seg.TableName, &seg.Path, &seg.SHA256, &seg.ContentHash, &seg.SizeBytes, &seg.RawBytes, &seg.SourceBytes, &seg.Rows, &seg.MinTime, &seg.MaxTime, &seg.MinID, &seg.MaxID, &seg.Status, &seg.SourceDeletedRows, &seg.DeleteWatermark, &created, &verified, &destroyed); err != nil {
			return nil, err
		}
		seg.CreatedAt = fromMS(created)
		seg.VerifiedAt, seg.DestroyedAt = fromMSPtr(verified), fromMSPtr(destroyed)
		out = append(out, seg)
	}
	return out, rows.Err()
}

func (s *Store) StorageTotals(ctx context.Context) (segments int, rows, sizeBytes, rawBytes int64, minT, maxT int64, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(rows),0), COALESCE(SUM(size_bytes),0), COALESCE(SUM(raw_bytes),0), COALESCE(MIN(min_time),0), COALESCE(MAX(max_time),0) FROM segments WHERE status='active'`).Scan(&segments, &rows, &sizeBytes, &rawBytes, &minT, &maxT)
	return
}

// ---------------- Holds ----------------

func (s *Store) CreateHold(ctx context.Context, h *Hold) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `INSERT INTO holds(case_no,reason,scope,target_value,target_from,target_to,time_from,time_to,active,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,1,?,?)`,
		h.CaseNo, h.Reason, h.Scope, h.TargetValue, h.TargetFrom, h.TargetTo, msPtr(h.TimeFrom), msPtr(h.TimeTo), h.CreatedBy, ms(now))
	if err != nil {
		return err
	}
	id, _ := res.LastInsertId()
	h.ID, h.Active, h.CreatedAt = id, true, now
	return nil
}

func (s *Store) ReleaseHold(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE holds SET active=0, released_at=? WHERE id=? AND active=1`, ms(time.Now().UTC()), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("hold %d not found or already released", id)
	}
	return nil
}

func (s *Store) Holds(ctx context.Context, activeOnly bool) ([]Hold, error) {
	q := `SELECT id,case_no,reason,scope,target_value,target_from,target_to,time_from,time_to,active,created_by,created_at,released_at FROM holds`
	if activeOnly {
		q += ` WHERE active=1`
	}
	q += ` ORDER BY id DESC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Hold
	for rows.Next() {
		var h Hold
		var created int64
		var timeFrom, timeTo, released sql.NullInt64
		var active int
		if err := rows.Scan(&h.ID, &h.CaseNo, &h.Reason, &h.Scope, &h.TargetValue, &h.TargetFrom, &h.TargetTo, &timeFrom, &timeTo, &active, &h.CreatedBy, &created, &released); err != nil {
			return nil, err
		}
		h.Active = active != 0
		h.CreatedAt = fromMS(created)
		h.TimeFrom, h.TimeTo = fromMSPtr(timeFrom), fromMSPtr(timeTo)
		h.ReleasedAt = fromMSPtr(released)
		out = append(out, h)
	}
	return out, rows.Err()
}

// ---------------- Manifests ----------------

func (s *Store) CreateManifest(ctx context.Context, m *Manifest) error {
	res, err := s.db.ExecContext(ctx, `INSERT INTO manifests(job_id,policy_id,table_name,created_at,cutoff,rows_destroyed,rows_held,bytes_freed,segments,id_sha256,file_path,file_sha256,file_bytes,summary)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		m.JobID, m.PolicyID, m.TableName, ms(m.CreatedAt), ms(m.Cutoff), m.RowsDestroyed, m.RowsHeld, m.BytesFreed, m.Segments, m.IDSHA256, m.FilePath, m.FileSHA256, m.FileBytes, m.Summary)
	if err != nil {
		return err
	}
	id, _ := res.LastInsertId()
	m.ID = id
	return nil
}

func (s *Store) AddManifestSegment(ctx context.Context, ms_ *ManifestSegment) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO manifest_segments(manifest_id,segment_id,path,rows_total,rows_destroyed,rows_retained,bytes_freed,result) VALUES(?,?,?,?,?,?,?,?)`,
		ms_.ManifestID, ms_.SegmentID, ms_.Path, ms_.RowsTotal, ms_.RowsDestroyed, ms_.RowsRetained, ms_.BytesFreed, ms_.Result)
	return err
}

func (s *Store) Manifests(ctx context.Context, limit int) ([]Manifest, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,job_id,policy_id,table_name,created_at,cutoff,rows_destroyed,rows_held,bytes_freed,segments,id_sha256,file_path,file_sha256,file_bytes,summary FROM manifests ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Manifest
	for rows.Next() {
		var m Manifest
		var created, cutoff int64
		if err := rows.Scan(&m.ID, &m.JobID, &m.PolicyID, &m.TableName, &created, &cutoff, &m.RowsDestroyed, &m.RowsHeld, &m.BytesFreed, &m.Segments, &m.IDSHA256, &m.FilePath, &m.FileSHA256, &m.FileBytes, &m.Summary); err != nil {
			return nil, err
		}
		m.CreatedAt, m.Cutoff = fromMS(created), fromMS(cutoff)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) Manifest(ctx context.Context, id int64) (*Manifest, error) {
	all, err := s.Manifests(ctx, 100000)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].ID == id {
			return &all[i], nil
		}
	}
	return nil, fmt.Errorf("manifest %d not found", id)
}

func (s *Store) ManifestSegments(ctx context.Context, id int64) ([]ManifestSegment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT manifest_id,segment_id,path,rows_total,rows_destroyed,rows_retained,bytes_freed,result FROM manifest_segments WHERE manifest_id=? ORDER BY segment_id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ManifestSegment
	for rows.Next() {
		var m ManifestSegment
		if err := rows.Scan(&m.ManifestID, &m.SegmentID, &m.Path, &m.RowsTotal, &m.RowsDestroyed, &m.RowsRetained, &m.BytesFreed, &m.Result); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) SetSetting(ctx context.Context, k, v string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, k, v)
	return err
}

func (s *Store) GetSetting(ctx context.Context, k string) (string, bool) {
	var v string
	if err := s.db.QueryRowContext(ctx, `SELECT v FROM settings WHERE k=?`, k).Scan(&v); err != nil {
		return "", false
	}
	return v, true
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// NormalizeTableName guards against SQL injection in table names used by the
// source adapter (identifiers cannot be parameterized).
func NormalizeTableName(t string) (string, error) {
	if t == "" || len(t) > 63 {
		return "", fmt.Errorf("invalid table name")
	}
	for _, r := range t {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return "", fmt.Errorf("invalid table name %q", t)
		}
	}
	return t, nil
}

func (s *Store) DumpJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
