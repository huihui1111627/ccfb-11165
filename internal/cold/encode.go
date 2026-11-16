package cold

import (
	"encoding/binary"
	"fmt"
	"math"

	"payment-archive/internal/model"
)

// Column encodings.
const (
	encIntPlain byte = 0 // zigzag varint
	encIntDelta byte = 1 // first zigzag, then zigzag(delta)
	encIntDoD   byte = 2 // delta-of-delta
	encStrPlain byte = 3
	encStrDict  byte = 4
)

// bitmap is a simple null bitmap: bit set means NULL.
type bitmap []byte

func newBitmap(n int) bitmap { return make(bitmap, (n+7)/8) }

func (b bitmap) set(i int) { b[i>>3] |= 1 << uint(i&7) }

func (b bitmap) get(i int) bool { return b[i>>3]&(1<<uint(i&7)) != 0 }

func (b bitmap) any() bool {
	for _, x := range b {
		if x != 0 {
			return true
		}
	}
	return false
}

func uvarintSize(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

func zigzag(v int64) uint64 {
	return uint64(v<<1) ^ uint64(v>>63)
}

func unzigzag(u uint64) int64 {
	return int64(u>>1) ^ -int64(u&1)
}

func putVarint(dst []byte, v int64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], zigzag(v))
	return append(dst, buf[:n]...)
}

func readVarint(src []byte, pos int) (int64, int, error) {
	u, n := binary.Uvarint(src[pos:])
	if n <= 0 {
		return 0, pos, fmt.Errorf("invalid varint at %d", pos)
	}
	return unzigzag(u), pos + n, nil
}

// encodeInts encodes compacted (non-null) values with the requested scheme.
func encodeInts(vals []int64, enc byte) []byte {
	out := make([]byte, 0, len(vals)*2)
	if len(vals) == 0 {
		return out
	}
	switch enc {
	case encIntPlain:
		for _, v := range vals {
			out = putVarint(out, v)
		}
	case encIntDelta:
		prev := int64(0)
		for i, v := range vals {
			if i == 0 {
				out = putVarint(out, v)
			} else {
				out = putVarint(out, v-prev)
			}
			prev = v
		}
	case encIntDoD:
		var prev, prevDelta int64
		for i, v := range vals {
			switch i {
			case 0:
				out = putVarint(out, v)
			case 1:
				prevDelta = v - prev
				out = putVarint(out, prevDelta)
			default:
				d := v - prev
				out = putVarint(out, d-prevDelta)
				prevDelta = d
			}
			prev = v
		}
	}
	return out
}

func decodeInts(src []byte, n int, enc byte) ([]int64, error) {
	out := make([]int64, n)
	pos := 0
	switch enc {
	case encIntPlain:
		for i := range out {
			v, np, err := readVarint(src, pos)
			if err != nil {
				return nil, err
			}
			out[i] = v
			pos = np
		}
	case encIntDelta:
		var prev int64
		for i := range out {
			v, np, err := readVarint(src, pos)
			if err != nil {
				return nil, err
			}
			pos = np
			if i == 0 {
				out[i] = v
			} else {
				out[i] = prev + v
			}
			prev = out[i]
		}
	case encIntDoD:
		var prev, prevDelta int64
		for i := range out {
			v, np, err := readVarint(src, pos)
			if err != nil {
				return nil, err
			}
			pos = np
			switch i {
			case 0:
				out[i] = v
			case 1:
				prevDelta = v
				out[i] = prev + v
			default:
				prevDelta += v
				out[i] = prev + prevDelta
			}
			prev = out[i]
		}
	default:
		return nil, fmt.Errorf("unknown int encoding %d", enc)
	}
	return out, nil
}

func encodeStrings(vals []string) (enc byte, data []byte, dictSize int) {
	plain := make([]byte, 0, len(vals)*12)
	for _, s := range vals {
		plain = appendUvarint(plain, uint64(len(s)))
		plain = append(plain, s...)
	}
	// Dictionary encoding, capped to keep lookups cheap.
	idx := make(map[string]uint32, 256)
	var dict []string
	indices := make([]uint32, len(vals))
	for i, s := range vals {
		id, ok := idx[s]
		if !ok {
			id = uint32(len(dict))
			idx[s] = id
			dict = append(dict, s)
		}
		indices[i] = id
	}
	dictData := make([]byte, 0, len(plain)/2)
	dictData = appendUvarint(dictData, uint64(len(dict)))
	for _, s := range dict {
		dictData = appendUvarint(dictData, uint64(len(s)))
		dictData = append(dictData, s...)
	}
	for _, id := range indices {
		dictData = appendUvarint(dictData, uint64(id))
	}
	if len(dict) <= 65535 && len(dictData) < len(plain) {
		return encStrDict, dictData, len(dict)
	}
	return encStrPlain, plain, len(dict)
}

func appendUvarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func decodeStrings(src []byte, n int, enc byte) ([]string, error) {
	out := make([]string, n)
	pos := 0
	switch enc {
	case encStrPlain:
		for i := range out {
			l, np, err := readUvarint(src, pos)
			if err != nil {
				return nil, err
			}
			pos = np
			if pos+int(l) > len(src) {
				return nil, fmt.Errorf("string overrun")
			}
			out[i] = string(src[pos : pos+int(l)])
			pos += int(l)
		}
	case encStrDict:
		cnt, np, err := readUvarint(src, pos)
		if err != nil {
			return nil, err
		}
		pos = np
		if cnt > 1<<20 {
			return nil, fmt.Errorf("absurd dictionary size %d", cnt)
		}
		dict := make([]string, cnt)
		for i := 0; i < int(cnt); i++ {
			l, np, err := readUvarint(src, pos)
			if err != nil {
				return nil, err
			}
			pos = np
			if pos+int(l) > len(src) {
				return nil, fmt.Errorf("dictionary overrun")
			}
			dict[i] = string(src[pos : pos+int(l)])
			pos += int(l)
		}
		for i := range out {
			id, np, err := readUvarint(src, pos)
			if err != nil {
				return nil, err
			}
			pos = np
			if int(id) >= len(dict) {
				return nil, fmt.Errorf("dictionary index %d out of range", id)
			}
			out[i] = dict[id]
		}
	default:
		return nil, fmt.Errorf("unknown string encoding %d", enc)
	}
	return out, nil
}

func readUvarint(src []byte, pos int) (uint64, int, error) {
	if pos >= len(src) {
		return 0, pos, fmt.Errorf("unexpected end of varint")
	}
	v, n := binary.Uvarint(src[pos:])
	if n <= 0 {
		return 0, pos, fmt.Errorf("invalid uvarint at %d", pos)
	}
	return v, pos + n, nil
}

// DecodedColumn holds one decoded column of a block.
type DecodedColumn struct {
	Kind model.Kind
	Ints []int64
	Strs []string
	Null []bool
	// min/max for int columns
	Min, Max int64
	HasInt   bool
}

// DecodedBlock is a decoded columnar block.
type DecodedBlock struct {
	Rows int
	Cols []DecodedColumn // indexed by schema column position
	// Size is an approximation of decoded memory, used by the cache and the
	// per-query byte budget.
	Size int64
}

func encodeBlock(rows []model.Row, schema *model.Schema) ([]byte, map[string]colStats, error) {
	n := len(rows)
	if n == 0 {
		return nil, nil, fmt.Errorf("empty block")
	}
	stats := make(map[string]colStats, len(schema.Columns))
	buf := make([]byte, 0, n*48)
	for ci, col := range schema.Columns {
		bm := newBitmap(n)
		hasNull := false
		switch col.Kind {
		case model.KindInt64, model.KindTimestamp:
			vals := make([]int64, 0, n)
			st := colStats{HasInt: true, Min: math.MaxInt64, Max: math.MinInt64}
			for ri, row := range rows {
				v := row[ci]
				if v.Null {
					bm.set(ri)
					hasNull = true
					continue
				}
				vals = append(vals, v.I)
				if v.I < st.Min {
					st.Min = v.I
				}
				if v.I > st.Max {
					st.Max = v.I
				}
			}
			if len(vals) == 0 {
				st.Min, st.Max = 0, 0
			}
			st.NullCount = int64(n - len(vals))
			stats[col.Name] = st

			plain := encodeInts(vals, encIntPlain)
			bestEnc := encIntPlain
			best := plain
			if len(vals) > 1 {
				delta := encodeInts(vals, encIntDelta)
				if len(delta) < len(best) {
					bestEnc, best = encIntDelta, delta
				}
				dod := encodeInts(vals, encIntDoD)
				if len(dod) < len(best) {
					bestEnc, best = encIntDoD, dod
				}
			}
			buf = appendColumnChunk(buf, bestEnc, bm, hasNull, best)
		case model.KindString:
			vals := make([]string, 0, n)
			distinct := make(map[string]struct{}, 64)
			for ri, row := range rows {
				v := row[ci]
				if v.Null {
					bm.set(ri)
					hasNull = true
					continue
				}
				vals = append(vals, v.S)
				if len(distinct) < 10000 {
					distinct[v.S] = struct{}{}
				}
			}
			enc, data, _ := encodeStrings(vals)
			stats[col.Name] = colStats{NullCount: int64(n - len(vals)), Distinct: int64(len(distinct))}
			buf = appendColumnChunk(buf, enc, bm, hasNull, data)
		default:
			return nil, nil, fmt.Errorf("unsupported kind %v", col.Kind)
		}
	}
	return buf, stats, nil
}

func appendColumnChunk(buf []byte, enc byte, bm bitmap, hasNull bool, data []byte) []byte {
	var flags byte
	if hasNull {
		flags = 1
	}
	buf = append(buf, flags, enc)
	if hasNull {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(bm)))
		buf = append(buf, bm...)
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(data)))
	buf = append(buf, data...)
	return buf
}

func decodeBlock(src []byte, n int, schema *model.Schema) (*DecodedBlock, error) {
	blk := &DecodedBlock{Rows: n, Cols: make([]DecodedColumn, len(schema.Columns))}
	pos := 0
	for ci, col := range schema.Columns {
		if pos+2 > len(src) {
			return nil, fmt.Errorf("truncated column header for %s", col.Name)
		}
		flags := src[pos]
		enc := src[pos+1]
		pos += 2
		var bm bitmap
		if flags&1 != 0 {
			if pos+4 > len(src) {
				return nil, fmt.Errorf("truncated bitmap length")
			}
			l := int(binary.LittleEndian.Uint32(src[pos:]))
			pos += 4
			if pos+l > len(src) || l != len(newBitmap(n)) {
				return nil, fmt.Errorf("bad bitmap length %d", l)
			}
			bm = bitmap(src[pos : pos+l])
			pos += l
		}
		if pos+4 > len(src) {
			return nil, fmt.Errorf("truncated data length")
		}
		l := int(binary.LittleEndian.Uint32(src[pos:]))
		pos += 4
		if pos+l > len(src) {
			return nil, fmt.Errorf("column data overrun")
		}
		data := src[pos : pos+l]
		pos += l

		hasBM := bm != nil
		nonNull := n
		if hasBM {
			nonNull = 0
			for i := 0; i < n; i++ {
				if !bm.get(i) {
					nonNull++
				}
			}
		}
		dc := DecodedColumn{Kind: col.Kind}
		switch col.Kind {
		case model.KindInt64, model.KindTimestamp:
			vals, err := decodeInts(data, nonNull, enc)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", col.Name, err)
			}
			dc.Ints = make([]int64, n)
			vi := 0
			for i := 0; i < n; i++ {
				if hasBM && bm.get(i) {
					continue
				}
				dc.Ints[i] = vals[vi]
				vi++
				if vi == 1 || dc.Ints[i] < dc.Min {
					dc.Min = dc.Ints[i]
				}
				if vi == 1 || dc.Ints[i] > dc.Max {
					dc.Max = dc.Ints[i]
				}
			}
			dc.HasInt = nonNull > 0
		case model.KindString:
			vals, err := decodeStrings(data, nonNull, enc)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", col.Name, err)
			}
			dc.Strs = make([]string, n)
			vi := 0
			for i := 0; i < n; i++ {
				if hasBM && bm.get(i) {
					continue
				}
				dc.Strs[i] = vals[vi]
				vi++
			}
		default:
			return nil, fmt.Errorf("unsupported kind %v", col.Kind)
		}
		if hasBM {
			dc.Null = make([]bool, n)
			for i := 0; i < n; i++ {
				dc.Null[i] = bm.get(i)
			}
		}
		blk.Cols[ci] = dc
	}
	if pos != len(src) {
		return nil, fmt.Errorf("trailing bytes in block: %d of %d consumed", pos, len(src))
	}
	for i := range blk.Cols {
		c := &blk.Cols[i]
		if c.Kind == model.KindString {
			blk.Size += int64(n * 16)
			for _, s := range c.Strs {
				blk.Size += int64(len(s))
			}
		} else {
			blk.Size += int64(n * 8)
		}
	}
	return blk, nil
}

type colStats struct {
	HasInt    bool
	Min, Max  int64
	NullCount int64
	Distinct  int64
}
