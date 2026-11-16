// Package coldquery serves queries over cold segments with block pruning,
// per-query byte budgets, bounded concurrency and rate limiting.
package coldquery

import (
	"container/list"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"payment-archive/internal/cold"
	"payment-archive/internal/config"
	"payment-archive/internal/control"
	"payment-archive/internal/model"
)

// Stats describes one cold query execution.
type Stats struct {
	SegmentsConsidered int           `json:"segments_considered"`
	SegmentsScanned    int           `json:"segments_scanned"`
	BlocksTotal        int           `json:"blocks_total"`
	BlocksPruned       int           `json:"blocks_pruned"`
	BlocksScanned      int           `json:"blocks_scanned"`
	BytesRead          int64         `json:"bytes_read"`
	RowsScanned        int64         `json:"rows_scanned"`
	RowsMatched        int64         `json:"rows_matched"`
	CacheHits          int           `json:"cache_hits"`
	CacheMisses        int           `json:"cache_misses"`
	Duration           time.Duration `json:"duration"`
	Throttled          bool          `json:"throttled"`
}

// Engine owns segment readers and the block cache.
type Engine struct {
	cfg     config.ColdQuery
	meta    *control.Store
	dec     *zstd.Decoder
	sem     chan struct{}
	rl      *rateLimiter
	cache   *blockCache
	stats   Metrics
	segMu   sync.Mutex
	readers map[string]*cold.SegmentReader
}

// Metrics are process-wide cold query counters exposed in the UI.
type Metrics struct {
	Total          atomic.Int64
	Throttled      atomic.Int64
	Failed         atomic.Int64
	RowsServed     atomic.Int64
	BlocksScanned  atomic.Int64
	BlocksPruned   atomic.Int64
	BytesRead      atomic.Int64
	TotalLatencyUS atomic.Int64
	MaxLatencyUS   atomic.Int64
}

func New(cfg config.ColdQuery, meta *control.Store) (*Engine, error) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 4
	}
	e := &Engine{
		cfg:     cfg,
		meta:    meta,
		dec:     dec,
		sem:     make(chan struct{}, cfg.MaxConcurrent),
		rl:      newRateLimiter(cfg.MaxQPS),
		cache:   newBlockCache(512 << 20),
		readers: make(map[string]*cold.SegmentReader),
	}
	return e, nil
}

func (e *Engine) Metrics() *Metrics { return &e.stats }
func (e *Engine) CacheStats() (blocks int, bytes int64, hits, misses int64) {
	return e.cache.stats()
}

func (e *Engine) reader(path string) (*cold.SegmentReader, error) {
	e.segMu.Lock()
	defer e.segMu.Unlock()
	if r, ok := e.readers[path]; ok {
		return r, nil
	}
	r, err := cold.OpenSegment(path)
	if err != nil {
		return nil, err
	}
	e.readers[path] = r
	return r, nil
}

// Invalidate drops a cached reader (called after a segment is rewritten or
// destroyed).
func (e *Engine) Invalidate(path string) {
	e.segMu.Lock()
	delete(e.readers, path)
	e.segMu.Unlock()
	e.cache.deletePath(path)
}

// Query runs a cold-only query. It returns the matching rows in descending
// (created_at,id) order, capped at limit. maxKeyTime/maxKeyID let the caller
// bound the scan for pagination.
func (e *Engine) Query(ctx context.Context, q model.Query, table string, limit int, maxKey *model.Key) ([]model.Row, Stats, error) {
	start := time.Now()
	var st Stats
	// Rate limit first: cheap reject without occupying a concurrency slot.
	if !e.rl.allow() {
		e.stats.Throttled.Add(1)
		return nil, st, &ThrottledError{Reason: "cold query rate limit exceeded"}
	}
	timer := time.NewTimer(time.Duration(e.cfg.QueueTimeoutMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case e.sem <- struct{}{}:
		defer func() { <-e.sem }()
	case <-timer.C:
		e.stats.Throttled.Add(1)
		return nil, st, &ThrottledError{Reason: "cold query concurrency limit reached"}
	case <-ctx.Done():
		return nil, st, ctx.Err()
	}

	segs, err := e.meta.ActiveSegments(ctx, table)
	if err != nil {
		e.stats.Failed.Add(1)
		return nil, st, err
	}
	type scanRef struct {
		seg  control.Segment
		path string
	}
	var refs []scanRef
	for _, seg := range segs {
		// Segment-level time pruning against the query range and cursor.
		if seg.MaxTime < q.From.UnixMicro() || seg.MinTime > q.To.UnixMicro() {
			continue
		}
		if maxKey != nil && seg.MinTime > maxKey.Time.UnixMicro() {
			continue
		}
		if maxKey != nil && seg.MinTime == maxKey.Time.UnixMicro() && seg.MinID > maxKey.ID {
			continue
		}
		refs = append(refs, scanRef{seg: seg, path: seg.Path})
	}
	st.SegmentsConsidered = len(refs)
	// Scan newest segments first so we can stop early once we have enough rows.
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].seg.MaxTime != refs[j].seg.MaxTime {
			return refs[i].seg.MaxTime > refs[j].seg.MaxTime
		}
		return refs[i].seg.ID > refs[j].seg.ID
	})

	budget := e.cfg.MaxBytesPerQuery
	var out []model.Row
	// If a row was updated between two archive runs, old and new versions can
	// both live in cold storage. Segments are scanned newest-first, so the
	// first occurrence of a key is the freshest version; later ones are dropped.
	seen := make(map[int64]struct{}, limit)
	for _, ref := range refs {
		if int64(len(out)) >= int64(limit) {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, st, err
		}
		r, err := e.reader(ref.path)
		if err != nil {
			// A missing/corrupt segment must not silently drop data; report it.
			e.stats.Failed.Add(1)
			return nil, st, fmt.Errorf("open segment %s: %w", ref.path, err)
		}
		st.SegmentsScanned++
		blocks := r.Blocks()
		// Newest blocks first.
		for bi := len(blocks) - 1; bi >= 0; bi-- {
			b := blocks[bi]
			st.BlocksTotal++
			if b.MaxTime < q.From.UnixMicro() || b.MinTime > q.To.UnixMicro() {
				st.BlocksPruned++
				continue
			}
			if maxKey != nil {
				if b.MinTime > maxKey.Time.UnixMicro() {
					st.BlocksPruned++
					continue
				}
				if b.MinTime == maxKey.Time.UnixMicro() && b.MinID > maxKey.ID {
					st.BlocksPruned++
					continue
				}
			}
			if !pruneBlock(q, r.Meta().Schema, &b) {
				st.BlocksPruned++
				continue
			}
			key := fmt.Sprintf("%s#%d", ref.path, bi)
			blk, ok, size := e.cache.get(key)
			if ok {
				st.CacheHits++
			} else {
				st.CacheMisses++
				blk, err = r.ReadBlock(bi, e.dec)
				if err != nil {
					e.stats.Failed.Add(1)
					return nil, st, fmt.Errorf("read block %s#%d: %w", ref.path, bi, err)
				}
				size = blk.Size
				e.cache.put(key, ref.path, blk, size)
			}
			st.BytesRead += b.CompLen
			budget -= b.RawLen
			st.BlocksScanned++
			st.RowsScanned += int64(blk.Rows)
			rows := matchBlock(blk, r.Meta().Schema, q, maxKey, limit-len(out))
			for _, row := range rows {
				id := row[0].I
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				out = append(out, row)
				if len(out) >= limit {
					break
				}
			}
			if int64(len(out)) >= int64(limit) || budget <= 0 {
				break
			}
		}
	}
	sortRowsDesc(out)
	if int64(len(out)) > int64(limit) {
		out = out[:limit]
	}

	st.RowsMatched = int64(len(out))
	st.Duration = time.Since(start)
	e.stats.Total.Add(1)
	e.stats.RowsServed.Add(int64(len(out)))
	e.stats.BlocksScanned.Add(int64(st.BlocksScanned))
	e.stats.BlocksPruned.Add(int64(st.BlocksPruned))
	e.stats.BytesRead.Add(st.BytesRead)
	us := st.Duration.Microseconds()
	e.stats.TotalLatencyUS.Add(us)
	for {
		cur := e.stats.MaxLatencyUS.Load()
		if us <= cur || e.stats.MaxLatencyUS.CompareAndSwap(cur, us) {
			break
		}
	}
	return out, st, nil
}

func sortRowsDesc(rows []model.Row) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i][10].I != rows[j][10].I {
			return rows[i][10].I > rows[j][10].I
		}
		return rows[i][0].I > rows[j][0].I
	})
}

func matchBlock(blk *cold.DecodedBlock, schema *model.Schema, q model.Query, maxKey *model.Key, want int) []model.Row {
	if want <= 0 {
		return nil
	}
	var out []model.Row
	fromUS, toUS := q.From.UnixMicro(), q.To.UnixMicro()
	for i := 0; i < blk.Rows; i++ {
		t := blk.Cols[10].Ints[i]
		if t < fromUS || t > toUS {
			continue
		}
		id := blk.Cols[0].Ints[i]
		if maxKey != nil {
			if t > maxKey.Time.UnixMicro() {
				continue
			}
			if t == maxKey.Time.UnixMicro() && id >= maxKey.ID {
				continue
			}
		}
		if !matchRow(blk, q, i) {
			continue
		}
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
		out = append(out, row)
	}
	// Rows inside a block are stored in ascending (created_at,id) order. The
	// caller needs the *newest* matches, so sort before applying the cap; a cap
	// applied in storage order would silently drop the newest rows and make
	// paginated results differ from a direct source query.
	if len(out) > 1 {
		sortRowsDesc(out)
	}
	if len(out) > want {
		out = out[:want]
	}
	return out
}

func matchRow(blk *cold.DecodedBlock, q model.Query, i int) bool {
	if q.MerchantID != nil && blk.Cols[2].Ints[i] != *q.MerchantID {
		return false
	}
	if q.Status != "" && blk.Cols[6].Strs[i] != q.Status {
		return false
	}
	if q.Channel != "" && blk.Cols[7].Strs[i] != q.Channel {
		return false
	}
	if q.Region != "" && blk.Cols[8].Strs[i] != q.Region {
		return false
	}
	if q.Currency != "" && blk.Cols[5].Strs[i] != q.Currency {
		return false
	}
	if q.TxnNo != "" && blk.Cols[1].Strs[i] != q.TxnNo {
		return false
	}
	if q.MaxID != nil && blk.Cols[0].Ints[i] > *q.MaxID {
		return false
	}
	if q.MinAmount != nil && blk.Cols[4].Ints[i] < *q.MinAmount {
		return false
	}
	if q.MaxAmount != nil && blk.Cols[4].Ints[i] > *q.MaxAmount {
		return false
	}
	return true
}

// pruneBlock returns true when the block must be scanned, false when block
// statistics prove no row can match.
func pruneBlock(q model.Query, schema *model.Schema, b *cold.BlockIndex) bool {
	if q.MerchantID != nil {
		ci := schema.Index("merchant_id")
		if ci >= 0 && (b.Cols[ci].MinI > *q.MerchantID || b.Cols[ci].MaxI < *q.MerchantID) {
			return false
		}
	}
	if q.MinAmount != nil {
		ci := schema.Index("amount_cents")
		if ci >= 0 && b.Cols[ci].MaxI < *q.MinAmount {
			return false
		}
	}
	if q.MaxAmount != nil {
		ci := schema.Index("amount_cents")
		if ci >= 0 && b.Cols[ci].MinI > *q.MaxAmount {
			return false
		}
	}
	if q.MaxID != nil {
		ci := schema.Index("id")
		if ci >= 0 && b.Cols[ci].MinI > *q.MaxID {
			return false
		}
	}
	// String min/max pruning (lexicographic) for low-cardinality equality
	// filters.
	if q.Status != "" {
		ci := schema.Index("status")
		if ci >= 0 && (q.Status < b.Cols[ci].MinS || q.Status > b.Cols[ci].MaxS) {
			return false
		}
	}
	if q.Channel != "" {
		ci := schema.Index("channel")
		if ci >= 0 && (q.Channel < b.Cols[ci].MinS || q.Channel > b.Cols[ci].MaxS) {
			return false
		}
	}
	if q.Region != "" {
		ci := schema.Index("region")
		if ci >= 0 && (q.Region < b.Cols[ci].MinS || q.Region > b.Cols[ci].MaxS) {
			return false
		}
	}
	if q.Currency != "" {
		ci := schema.Index("currency")
		if ci >= 0 && (q.Currency < b.Cols[ci].MinS || q.Currency > b.Cols[ci].MaxS) {
			return false
		}
	}
	return true
}

// ThrottledError signals admission-control rejection.
type ThrottledError struct{ Reason string }

func (e *ThrottledError) Error() string { return e.Reason }

// rateLimiter is a GCRA-style leaky bucket with a small burst allowance.
type rateLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	burst    int
	tokens   float64
	last     time.Time
}

func newRateLimiter(qps int) *rateLimiter {
	if qps <= 0 {
		return &rateLimiter{interval: 0}
	}
	return &rateLimiter{interval: time.Second / time.Duration(qps), burst: qps, tokens: float64(qps), last: time.Now()}
}

func (r *rateLimiter) allow() bool {
	if r.interval == 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.tokens += float64(now.Sub(r.last)) / float64(r.interval)
	if r.tokens > float64(r.burst) {
		r.tokens = float64(r.burst)
	}
	r.last = now
	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}

// blockCache is an LRU of decoded blocks keyed by segment#block.
type blockCache struct {
	mu     sync.Mutex
	max    int64
	size   int64
	ll     *list.List
	items  map[string]*list.Element
	hits   int64
	misses int64
	byPath map[string]int
}

type cacheEntry struct {
	key  string
	path string
	blk  *cold.DecodedBlock
	size int64
}

func newBlockCache(maxBytes int64) *blockCache {
	return &blockCache{max: maxBytes, ll: list.New(), items: make(map[string]*list.Element), byPath: make(map[string]int)}
}

func (c *blockCache) get(key string) (*cold.DecodedBlock, bool, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		c.hits++
		e := el.Value.(*cacheEntry)
		return e.blk, true, e.size
	}
	c.misses++
	return nil, false, 0
}

func (c *blockCache) put(key, path string, blk *cold.DecodedBlock, size int64) {
	if size > c.max || size <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&cacheEntry{key: key, path: path, blk: blk, size: size})
	c.items[key] = el
	c.byPath[path]++
	c.size += size
	for c.size > c.max {
		back := c.ll.Back()
		if back == nil {
			break
		}
		e := back.Value.(*cacheEntry)
		c.ll.Remove(back)
		delete(c.items, e.key)
		c.size -= e.size
		c.byPath[e.path]--
	}
}

func (c *blockCache) deletePath(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.ll.Front(); el != nil; {
		next := el.Next()
		e := el.Value.(*cacheEntry)
		if e.path == path {
			c.ll.Remove(el)
			delete(c.items, e.key)
			c.size -= e.size
			c.byPath[path]--
		}
		el = next
	}
	delete(c.byPath, path)
}

func (c *blockCache) stats() (blocks int, bytes int64, hits, misses int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len(), c.size, c.hits, c.misses
}
