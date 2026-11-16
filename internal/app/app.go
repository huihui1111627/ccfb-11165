// Package app wires the archive platform together: engines, scheduler and the
// HTTP management API.
package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"payment-archive/internal/archive"
	"payment-archive/internal/coldquery"
	"payment-archive/internal/config"
	"payment-archive/internal/control"
	"payment-archive/internal/destruction"
	"payment-archive/internal/source"
	"payment-archive/internal/unifiedquery"
)

type App struct {
	Cfg     config.Config
	Source  *source.Source
	Meta    *control.Store
	Archive *archive.Engine
	Destroy *destruction.Engine
	Cold    *coldquery.Engine
	Unified *unifiedquery.Engine

	jobs   chan jobRequest
	closed bool
	once   sync.Once
	mu     sync.Mutex
	// active maps job id -> cancel func.
	active map[int64]context.CancelFunc
	wg     sync.WaitGroup
	stop   context.CancelFunc
}

type jobRequest struct {
	job    *control.Job
	policy *control.Policy
}

func New(ctx context.Context, cfg config.Config) (*App, error) {
	src, err := source.Open(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	if err := src.EnsureSchema(ctx); err != nil {
		src.Close()
		return nil, fmt.Errorf("source schema: %w", err)
	}
	meta, err := control.Open(cfg.ControlDB)
	if err != nil {
		src.Close()
		return nil, err
	}
	cold, err := coldquery.New(cfg.ColdQuery, meta)
	if err != nil {
		src.Close()
		meta.Close()
		return nil, err
	}
	a := &App{
		Cfg:    cfg,
		Source: src,
		Meta:   meta,
		Cold:   cold,
		jobs:   make(chan jobRequest, 64),
		active: make(map[int64]context.CancelFunc),
	}
	a.Unified = &unifiedquery.Engine{
		Source:       src,
		Cold:         cold,
		Table:        "transactions",
		MaxLimit:     cfg.ColdQuery.MaxLimit,
		DefaultLimit: cfg.ColdQuery.DefaultLimit,
	}
	a.Archive = archive.New(cfg, src, meta)
	a.Archive.Hooks = archive.Hooks{
		Log: func(level, msg string) { log.Printf("[archive] %s: %s", level, msg) },
	}
	a.Destroy = &destruction.Engine{
		Cfg:              cfg,
		Source:           src,
		Meta:             meta,
		Log:              func(jobID int64, level, msg string) { log.Printf("[destroy job %d] %s: %s", jobID, level, msg) },
		OnSegmentChanged: func(path string) { cold.Invalidate(path) },
	}
	_ = a.Archive.RecoverInterrupted(ctx)
	go a.worker()
	a.startScheduler()
	return a, nil
}

// Close stops the scheduler and job worker, then releases resources. It is
// safe to call more than once.
func (a *App) Close() {
	a.once.Do(func() {
		if a.stop != nil {
			a.stop()
		}
		a.mu.Lock()
		a.closed = true
		a.mu.Unlock()
		close(a.jobs)
		a.wg.Wait()
		a.Source.Close()
		a.Meta.Close()
	})
}

// Enqueue creates and queues a job of the given type. It returns an error if a
// conflicting job is already running for the same policy.
func (a *App) Enqueue(ctx context.Context, typ string, policyID int64) (*control.Job, error) {
	pol, err := a.Meta.Policy(ctx, policyID)
	if err != nil {
		return nil, err
	}
	if !pol.Enabled {
		return nil, fmt.Errorf("policy %d is disabled", policyID)
	}
	running, err := a.Meta.RunningJob(ctx, typ, policyID)
	if err != nil {
		return nil, err
	}
	if running {
		return nil, fmt.Errorf("a %s job for policy %d is already queued or running", typ, policyID)
	}
	job, err := a.Meta.CreateJob(ctx, typ, policyID)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("server is shutting down")
	}
	select {
	case a.jobs <- jobRequest{job: job, policy: pol}:
		return job, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *App) Cancel(jobID int64) error {
	a.mu.Lock()
	cancel, ok := a.active[jobID]
	a.mu.Unlock()
	if !ok {
		return errors.New("job is not running")
	}
	cancel()
	return nil
}

func (a *App) worker() {
	a.wg.Add(1)
	defer a.wg.Done()
	for req := range a.jobs {
		jobCtx, cancel := context.WithCancel(context.Background())
		a.mu.Lock()
		a.active[req.job.ID] = cancel
		a.mu.Unlock()
		var err error
		switch req.job.Type {
		case "archive":
			err = a.Archive.Run(jobCtx, req.job, req.policy)
		case "destroy":
			err = a.Destroy.Run(jobCtx, req.job, req.policy)
		case "verify":
			err = a.runVerify(jobCtx, req.job)
		default:
			err = fmt.Errorf("unknown job type %q", req.job.Type)
			_ = a.Meta.FinishJob(context.Background(), req.job.ID, "failed", err.Error())
		}
		if err != nil && jobCtx.Err() != nil {
			_ = a.Meta.FinishJob(context.Background(), req.job.ID, "cancelled", "cancelled by user")
		}
		a.mu.Lock()
		delete(a.active, req.job.ID)
		a.mu.Unlock()
		cancel()
	}
}

func (a *App) runVerify(ctx context.Context, job *control.Job) error {
	if err := a.Meta.StartJob(ctx, job.ID); err != nil {
		return err
	}
	if err := a.Archive.VerifyJob(ctx, job.ID); err != nil {
		_ = a.Meta.FinishJob(ctx, job.ID, "failed", err.Error())
		return err
	}
	return a.Meta.FinishJob(ctx, job.ID, "succeeded", "")
}

func (a *App) startScheduler() {
	interval := a.Cfg.SchedulerInterval.Std()
	if interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.stop = cancel
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				a.scheduleOnce(ctx)
			}
		}
	}()
}

func (a *App) scheduleOnce(ctx context.Context) {
	policies, err := a.Meta.Policies(ctx)
	if err != nil {
		log.Printf("[scheduler] policies: %v", err)
		return
	}
	for _, p := range policies {
		if !p.Enabled {
			continue
		}
		if a.Cfg.AutoArchive {
			due := p.LastRunAt == nil || time.Since(*p.LastRunAt) > a.Cfg.SchedulerInterval.Std()
			if due {
				if n, err := a.Source.CountCold(ctx, p.ColdAfterDays); err == nil && n > 0 {
					if _, err := a.Enqueue(ctx, "archive", p.ID); err != nil {
						log.Printf("[scheduler] enqueue archive: %v", err)
					} else {
						log.Printf("[scheduler] queued archive for policy %d (%d candidates)", p.ID, n)
					}
				}
			}
		}
		if a.Cfg.AutoDestroy {
			due := p.LastDestroyAt == nil || time.Since(*p.LastDestroyAt) > a.Cfg.SchedulerInterval.Std()
			if !due {
				continue
			}
			segs, err := a.Meta.ActiveSegments(ctx, p.TableName)
			if err != nil {
				continue
			}
			cutoff := time.Now().UTC().AddDate(0, 0, -p.DestroyAfterDays).UnixMicro()
			for _, s := range segs {
				if s.MaxTime < cutoff {
					if _, err := a.Enqueue(ctx, "destroy", p.ID); err != nil {
						log.Printf("[scheduler] enqueue destroy: %v", err)
					} else {
						log.Printf("[scheduler] queued destroy for policy %d", p.ID)
					}
					break
				}
			}
		}
	}
}

// WaitIdle blocks until all queued jobs have been processed (used by tests).
func (a *App) WaitIdle() {
	for {
		time.Sleep(50 * time.Millisecond)
		a.mu.Lock()
		n := len(a.active)
		a.mu.Unlock()
		if n == 0 {
			return
		}
	}
}
