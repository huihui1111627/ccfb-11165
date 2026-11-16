package cold

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"payment-archive/internal/model"
)

const (
	segMagic    = "PAYCOL01"
	segFooter   = "PAYIDX01"
	segVersion  = 1
	segMaxBlock = 8192
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// SegmentMeta is the immutable descriptor stored in the file header.
type SegmentMeta struct {
	Table            string        `json:"table"`
	Schema           *model.Schema `json:"schema"`
	CreatedAt        time.Time     `json:"created_at"`
	SourceRunID      string        `json:"source_run_id"`
	MinTime          int64         `json:"min_time_us"`
	MaxTime          int64         `json:"max_time_us"`
	Rows             int64         `json:"rows"`
	Compression      string        `json:"compression"`
	CompressionLevel int           `json:"compression_level"`
	AppVersion       string        `json:"app_version"`
}

// BlockIndex describes one compressed block inside a segment.
type BlockIndex struct {
	Offset  int64 `json:"off"`
	CompLen int64 `json:"clen"`
	RawLen  int64 `json:"rlen"`
	Rows    int   `json:"rows"`
	MinTime int64 `json:"min_t_us"`
	MaxTime int64 `json:"max_t_us"`
	MinID   int64 `json:"min_id"`
	MaxID   int64 `json:"max_id"`
	// Column statistics for predicate pruning, indexed by schema position.
	Cols []ColumnStats `json:"cols"`
}

// ColumnStats carries block-level min/max for pruning.
type ColumnStats struct {
	MinI int64  `json:"mini,omitempty"`
	MaxI int64  `json:"maxi,omitempty"`
	MinS string `json:"mins,omitempty"`
	MaxS string `json:"maxs,omitempty"`
}

// Footer is written at the end of the segment file.
type Footer struct {
	Blocks []BlockIndex `json:"blocks"`
	// SHA256 is filled on verification and is not part of the written footer.
	SHA256 string `json:"sha256,omitempty"`
}

// SegmentWriter streams compressed blocks into one immutable file.
type SegmentWriter struct {
	f      *os.File
	meta   SegmentMeta
	enc    *zstd.Encoder
	index  []BlockIndex
	offset int64
	hash   hashWriter
	buf    []byte
	total  int64
}

type hashWriter struct {
	h interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
}

// CreateSegment creates a new segment file, writing the header immediately.
func CreateSegment(path string, meta SegmentMeta, level int) (*SegmentWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	meta.Compression = "zstd"
	meta.CompressionLevel = level
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	sw := &SegmentWriter{f: f, meta: meta, enc: enc}
	sw.hash.h = sha256.New()
	if err := sw.writeHeader(); err != nil {
		sw.Abort()
		return nil, err
	}
	return sw, nil
}

func (w *SegmentWriter) writeHeader() error {
	metaJSON, err := json.Marshal(w.meta)
	if err != nil {
		return err
	}
	hdr := make([]byte, 0, len(metaJSON)+16)
	hdr = append(hdr, segMagic...)
	hdr = binary.LittleEndian.AppendUint32(hdr, segVersion)
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(len(metaJSON)))
	hdr = append(hdr, metaJSON...)
	if _, err := w.f.Write(hdr); err != nil {
		return err
	}
	w.hash.h.Write(hdr)
	w.offset = int64(len(hdr))
	return nil
}

// AppendBlock encodes rows (already in schema order) and writes a block.
func (w *SegmentWriter) AppendBlock(rows []model.Row, schema *model.Schema) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) > segMaxBlock {
		return fmt.Errorf("block too large: %d rows", len(rows))
	}
	raw, stats, err := encodeBlock(rows, schema)
	if err != nil {
		return err
	}
	comp := w.enc.EncodeAll(raw, nil)
	head := make([]byte, 12)
	binary.LittleEndian.PutUint32(head[0:], uint32(len(raw)))
	binary.LittleEndian.PutUint32(head[4:], uint32(len(comp)))
	binary.LittleEndian.PutUint32(head[8:], crc32.Checksum(comp, castagnoli))
	if _, err := w.f.Write(head); err != nil {
		return err
	}
	if _, err := w.f.Write(comp); err != nil {
		return err
	}
	w.hash.h.Write(head)
	w.hash.h.Write(comp)
	idx := BlockIndex{
		Offset:  w.offset,
		CompLen: int64(len(comp)),
		RawLen:  int64(len(raw)),
		Rows:    len(rows),
		Cols:    make([]ColumnStats, len(schema.Columns)),
	}
	idx.MinTime, idx.MaxTime = int64(1<<62), int64(-1<<62)
	idx.MinID, idx.MaxID = int64(1<<62), int64(-1<<62)
	timeIdx := schema.Index(schema.TimeField)
	pkIdx := schema.Index(schema.PKField)
	for i, row := range rows {
		t := row[timeIdx].I
		id := row[pkIdx].I
		if t < idx.MinTime {
			idx.MinTime = t
		}
		if t > idx.MaxTime {
			idx.MaxTime = t
		}
		if id < idx.MinID {
			idx.MinID = id
		}
		if id > idx.MaxID {
			idx.MaxID = id
		}
		if i == 0 {
			for ci, col := range schema.Columns {
				switch col.Kind {
				case model.KindString:
					if !row[ci].Null {
						idx.Cols[ci].MinS = row[ci].S
						idx.Cols[ci].MaxS = row[ci].S
					}
				default:
					if !row[ci].Null {
						idx.Cols[ci].MinI = row[ci].I
						idx.Cols[ci].MaxI = row[ci].I
					}
				}
			}
		}
		for ci, col := range schema.Columns {
			if row[ci].Null {
				continue
			}
			switch col.Kind {
			case model.KindString:
				if row[ci].S < idx.Cols[ci].MinS {
					idx.Cols[ci].MinS = row[ci].S
				}
				if row[ci].S > idx.Cols[ci].MaxS {
					idx.Cols[ci].MaxS = row[ci].S
				}
			default:
				if row[ci].I < idx.Cols[ci].MinI {
					idx.Cols[ci].MinI = row[ci].I
				}
				if row[ci].I > idx.Cols[ci].MaxI {
					idx.Cols[ci].MaxI = row[ci].I
				}
			}
		}
	}
	for ci, col := range schema.Columns {
		if col.Kind != model.KindString {
			if st, ok := stats[col.Name]; ok && !st.HasInt {
				idx.Cols[ci] = ColumnStats{}
			}
		}
	}
	idx.Offset = w.offset + 12
	w.offset += int64(12 + len(comp))
	w.index = append(w.index, idx)
	w.total += int64(len(rows))
	return nil
}

// Close writes the footer and fsyncs.
func (w *SegmentWriter) Close() (Footer, error) {
	foot := Footer{Blocks: w.index}
	if w.total != w.meta.Rows {
		w.Abort()
		return foot, fmt.Errorf("row count mismatch: header %d written %d", w.meta.Rows, w.total)
	}
	footJSON, err := json.Marshal(foot)
	if err != nil {
		w.Abort()
		return foot, err
	}
	tail := make([]byte, 0, len(footJSON)+12)
	tail = append(tail, footJSON...)
	tail = binary.LittleEndian.AppendUint32(tail, uint32(len(footJSON)))
	tail = binary.LittleEndian.AppendUint32(tail, crc32.Checksum(footJSON, castagnoli))
	tail = append(tail, segFooter...)
	if _, err := w.f.Write(tail); err != nil {
		w.Abort()
		return foot, err
	}
	w.hash.h.Write(tail)
	if err := w.f.Sync(); err != nil {
		w.Abort()
		return foot, err
	}
	foot.SHA256 = hex.EncodeToString(w.hash.h.Sum(nil))
	if err := w.f.Close(); err != nil {
		return foot, err
	}
	w.enc.Close()
	return foot, nil
}

// Abort closes and removes a partially written segment.
func (w *SegmentWriter) Abort() {
	w.enc.Close()
	w.f.Close()
	os.Remove(w.f.Name())
}

// SegmentReader reads a segment file.
type SegmentReader struct {
	path   string
	meta   SegmentMeta
	footer Footer
}

// OpenSegment opens and validates the header/footer of a segment.
func OpenSegment(path string) (*SegmentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < 16+int64(len(segMagic)) {
		return nil, fmt.Errorf("segment too small")
	}
	tail := make([]byte, 16)
	if _, err := f.ReadAt(tail, st.Size()-16); err != nil {
		return nil, err
	}
	if string(tail[8:]) != segFooter {
		return nil, fmt.Errorf("bad segment footer magic in %s", path)
	}
	footLen := int64(binary.LittleEndian.Uint32(tail[0:4]))
	footCRC := binary.LittleEndian.Uint32(tail[4:8])
	if footLen <= 0 || footLen > st.Size()-16 {
		return nil, fmt.Errorf("bad footer length %d", footLen)
	}
	footJSON := make([]byte, footLen)
	if _, err := f.ReadAt(footJSON, st.Size()-16-footLen); err != nil {
		return nil, err
	}
	if crc32.Checksum(footJSON, castagnoli) != footCRC {
		return nil, fmt.Errorf("footer checksum mismatch in %s", path)
	}
	var foot Footer
	if err := json.Unmarshal(footJSON, &foot); err != nil {
		return nil, err
	}
	hdr := make([]byte, len(segMagic)+8)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return nil, err
	}
	if string(hdr[:8]) != segMagic {
		return nil, fmt.Errorf("bad segment magic")
	}
	metaLen := int64(binary.LittleEndian.Uint32(hdr[12:16]))
	metaJSON := make([]byte, metaLen)
	if _, err := f.ReadAt(metaJSON, 16); err != nil {
		return nil, err
	}
	var meta SegmentMeta
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		return nil, err
	}
	if meta.Schema == nil || meta.Schema.TimeField == "" {
		return nil, fmt.Errorf("segment missing schema")
	}
	r := &SegmentReader{path: path, meta: meta, footer: foot}
	// Validate block offsets.
	for i, b := range foot.Blocks {
		if b.Offset < 16+metaLen || b.Offset+b.CompLen > st.Size()-16-footLen {
			return nil, fmt.Errorf("block %d offset out of range", i)
		}
	}
	return r, nil
}

func (r *SegmentReader) Path() string         { return r.path }
func (r *SegmentReader) Meta() SegmentMeta    { return r.meta }
func (r *SegmentReader) Blocks() []BlockIndex { return r.footer.Blocks }
func (r *SegmentReader) Rows() int64          { return r.meta.Rows }

// ReadBlock decodes the block at index i.
func (r *SegmentReader) ReadBlock(i int, dec *zstd.Decoder) (*DecodedBlock, error) {
	if i < 0 || i >= len(r.footer.Blocks) {
		return nil, fmt.Errorf("block index %d out of range", i)
	}
	b := r.footer.Blocks[i]
	f, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	head := make([]byte, 12)
	if _, err := f.ReadAt(head, b.Offset-12); err != nil {
		return nil, err
	}
	rawLen := int64(binary.LittleEndian.Uint32(head[0:4]))
	compLen := int64(binary.LittleEndian.Uint32(head[4:8]))
	crc := binary.LittleEndian.Uint32(head[8:12])
	if rawLen != b.RawLen || compLen != b.CompLen {
		return nil, fmt.Errorf("block %d index/header mismatch", i)
	}
	comp := make([]byte, compLen)
	if _, err := f.ReadAt(comp, b.Offset); err != nil {
		return nil, err
	}
	if crc32.Checksum(comp, castagnoli) != crc {
		return nil, fmt.Errorf("block %d CRC mismatch", i)
	}
	if dec == nil {
		var err error
		dec, err = zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer dec.Close()
	}
	raw, err := dec.DecodeAll(comp, make([]byte, 0, rawLen))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != rawLen {
		return nil, fmt.Errorf("block %d raw length mismatch", i)
	}
	return decodeBlock(raw, b.Rows, r.meta.Schema)
}

// VerifyStreaming re-reads the whole file, checks CRCs and decodes every block.
// It returns the number of rows and block count actually read.
func (r *SegmentReader) VerifyStreaming(dec *zstd.Decoder) (rows int64, blocks int, err error) {
	if dec == nil {
		dec, err = zstd.NewReader(nil)
		if err != nil {
			return 0, 0, err
		}
		defer dec.Close()
	}
	for i := range r.footer.Blocks {
		blk, err := r.ReadBlock(i, dec)
		if err != nil {
			return rows, i, err
		}
		rows += int64(blk.Rows)
		blocks++
	}
	if rows != r.meta.Rows {
		return rows, blocks, fmt.Errorf("row count mismatch on verify: meta %d read %d", r.meta.Rows, rows)
	}
	return rows, blocks, nil
}

// FileSHA256 streams the segment and hashes it.
func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
