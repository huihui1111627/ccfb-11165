// Package config loads runtime configuration for the archive platform.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Archive struct {
	BatchRows         int `json:"batch_rows"`
	DeleteChunkRows   int `json:"delete_chunk_rows"`
	DeletePauseMS     int `json:"delete_pause_ms"`
	TargetSegmentRows int `json:"target_segment_rows"`
	Workers           int `json:"workers"`
}

type ColdQuery struct {
	MaxConcurrent    int   `json:"max_concurrent"`
	MaxQPS           int   `json:"max_qps"`
	QueueTimeoutMS   int   `json:"queue_timeout_ms"`
	MaxBytesPerQuery int64 `json:"max_bytes_per_query"`
	MaxLimit         int   `json:"max_limit"`
	DefaultLimit     int   `json:"default_limit"`
}

type Demo struct {
	Rows            int64  `json:"rows"`
	Years           int    `json:"years"`
	ColdAfterDays   int    `json:"cold_after_days"`
	DestroyAfterDay int    `json:"destroy_after_days"`
	LiveWriteRPS    int    `json:"live_write_rps"`
	QueryCases      int    `json:"query_cases"`
	BenchIterations int    `json:"bench_iterations"`
	ReportDir       string `json:"report_dir"`
}

type Config struct {
	Listen                    string    `json:"listen"`
	DataDir                   string    `json:"data_dir"`
	ControlDB                 string    `json:"control_db"`
	SourceDSN                 string    `json:"source_dsn"`
	RegulatoryMinRetentionDay int       `json:"regulatory_min_retention_days"`
	SchedulerInterval         Duration  `json:"scheduler_interval"`
	AutoArchive               bool      `json:"auto_archive"`
	AutoDestroy               bool      `json:"auto_destroy"`
	Archive                   Archive   `json:"archive"`
	ColdQuery                 ColdQuery `json:"cold_query"`
	Demo                      Demo      `json:"demo"`
}

// Duration wraps time.Duration so it can be expressed as "5m" in JSON.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

func Default() Config {
	return Config{
		Listen:                    ":8080",
		DataDir:                   "./data",
		ControlDB:                 "./data/control.db",
		SourceDSN:                 "postgres:///payarchive?sslmode=disable",
		RegulatoryMinRetentionDay: 1825,
		SchedulerInterval:         Duration(5 * time.Minute),
		AutoArchive:               true,
		AutoDestroy:               true,
		Archive: Archive{
			BatchRows:         50000,
			DeleteChunkRows:   2000,
			DeletePauseMS:     5,
			TargetSegmentRows: 1000000,
			Workers:           0,
		},
		ColdQuery: ColdQuery{
			MaxConcurrent:    4,
			MaxQPS:           10,
			QueueTimeoutMS:   1000,
			MaxBytesPerQuery: 128 << 20,
			MaxLimit:         1000,
			DefaultLimit:     100,
		},
		Demo: Demo{
			Rows:            30_000_000,
			Years:           7,
			ColdAfterDays:   1095,
			DestroyAfterDay: 1825,
			LiveWriteRPS:    30,
			QueryCases:      24,
			BenchIterations: 40,
			ReportDir:       "./data/report",
		},
	}
}

// Load reads an optional JSON config file and applies environment overrides.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read config: %w", err)
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config: %w", err)
		}
	}
	if v := os.Getenv("PAYARCHIVE_LISTEN"); v != "" {
		cfg.Listen = v
	}
	if v := os.Getenv("PAYARCHIVE_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("PAYARCHIVE_SOURCE_DSN"); v != "" {
		cfg.SourceDSN = v
	}
	if v := os.Getenv("PAYARCHIVE_CONTROL_DB"); v != "" {
		cfg.ControlDB = v
	}
	if v := os.Getenv("PAYARCHIVE_SCHEDULER"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, err
		}
		cfg.SchedulerInterval = Duration(d)
	}
	if v := os.Getenv("PAYARCHIVE_DEMO_ROWS"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cfg, err
		}
		cfg.Demo.Rows = n
	}
	if cfg.Archive.Workers <= 0 {
		cfg.Archive.Workers = 8
	}
	if cfg.Archive.BatchRows <= 0 {
		cfg.Archive.BatchRows = 50000
	}
	if cfg.Archive.DeleteChunkRows <= 0 {
		cfg.Archive.DeleteChunkRows = 2000
	}
	if cfg.Archive.TargetSegmentRows <= 0 {
		cfg.Archive.TargetSegmentRows = 1_000_000
	}
	if cfg.ColdQuery.DefaultLimit <= 0 {
		cfg.ColdQuery.DefaultLimit = 100
	}
	if cfg.ColdQuery.MaxLimit <= 0 {
		cfg.ColdQuery.MaxLimit = 1000
	}
	return cfg, nil
}
