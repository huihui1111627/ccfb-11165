package cold

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"payment-archive/internal/model"
)

func makeRows(n int, seed int64) []model.Row {
	rng := rand.New(rand.NewSource(seed))
	base := time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	statuses := []string{"approved", "declined", "refunded", "pending"}
	channels := []string{"pos", "online", "mobile", "atm"}
	regions := []string{"APAC", "EMEA", "AMER", "LATAM"}
	currencies := []string{"USD", "EUR", "GBP", "SGD"}
	rows := make([]model.Row, n)
	for i := 0; i < n; i++ {
		r := make(model.Row, 12)
		id := int64(i + 1)
		r[0] = model.Value{I: id}
		r[1] = model.Value{S: "TXN" + time.UnixMicro(base+int64(i)*1000).Format("150405")}
		r[2] = model.Value{I: rng.Int63n(5000) + 1}
		r[3] = model.Value{S: "0123"}
		r[4] = model.Value{I: rng.Int63n(500000) + 100}
		r[5] = model.Value{S: currencies[i%4]}
		r[6] = model.Value{S: statuses[i%4]}
		r[7] = model.Value{S: channels[i%4]}
		r[8] = model.Value{S: regions[i%4]}
		r[9] = model.Value{I: int64(i % 100)}
		r[10] = model.Value{I: base + int64(i)*1000}
		if i%3 == 0 {
			r[11] = model.Value{Null: true}
		} else {
			r[11] = model.Value{S: "memo-" + statuses[i%4]}
		}
		rows[i] = r
	}
	return rows
}

func TestSegmentRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.seg")
	rows := makeRows(10000, 7)
	sw, err := CreateSegment(path, SegmentMeta{
		Table: "transactions", Schema: model.Transactions, CreatedAt: time.Now().UTC(),
		MinTime: rows[0][10].I, MaxTime: rows[len(rows)-1][10].I, Rows: int64(len(rows)),
	}, 5)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off < len(rows); off += 8192 {
		end := off + 8192
		if end > len(rows) {
			end = len(rows)
		}
		if err := sw.AppendBlock(rows[off:end], model.Transactions); err != nil {
			t.Fatal(err)
		}
	}
	foot, err := sw.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(foot.Blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(foot.Blocks))
	}
	r, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows() != int64(len(rows)) {
		t.Fatalf("rows=%d", r.Rows())
	}
	var got []model.Row
	for bi := 0; bi < len(r.Blocks()); bi++ {
		blk, err := r.ReadBlock(bi, nil)
		if err != nil {
			t.Fatalf("block %d: %v", bi, err)
		}
		for i := 0; i < blk.Rows; i++ {
			row := make(model.Row, 12)
			for ci, col := range model.Transactions.Columns {
				dc := &blk.Cols[ci]
				null := dc.Null != nil && dc.Null[i]
				if col.Kind == model.KindString {
					row[ci] = model.Value{S: dc.Strs[i], Null: null}
				} else {
					row[ci] = model.Value{I: dc.Ints[i], Null: null}
				}
			}
			got = append(got, row)
		}
	}
	if !reflect.DeepEqual(got, rows) {
		for i := range rows {
			if !reflect.DeepEqual(got[i], rows[i]) {
				t.Fatalf("row %d mismatch:\n got %+v\nwant %+v", i, got[i], rows[i])
			}
		}
	}
	// VerifyStreaming must succeed and FileSHA256 must match the footer.
	n, _, err := r.VerifyStreaming(nil)
	if err != nil || n != int64(len(rows)) {
		t.Fatalf("verify: n=%d err=%v", n, err)
	}
	sum, err := FileSHA256(path)
	if err != nil || sum != foot.SHA256 {
		t.Fatalf("sha mismatch: %v", err)
	}
}

func TestSegmentCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.seg")
	rows := makeRows(2000, 11)
	sw, err := CreateSegment(path, SegmentMeta{Table: "transactions", Schema: model.Transactions,
		CreatedAt: time.Now().UTC(), MinTime: rows[0][10].I, MaxTime: rows[len(rows)-1][10].I, Rows: int64(len(rows))}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := sw.AppendBlock(rows, model.Transactions); err != nil {
		t.Fatal(err)
	}
	if _, err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	// Corrupt the first block payload (header is 16 bytes + meta JSON).
	r, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	off := r.Blocks()[0].Offset
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[off+2] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.VerifyStreaming(nil); err == nil {
		t.Fatal("expected corruption to be detected")
	}
}

func TestEncodeDecodeInts(t *testing.T) {
	cases := [][]int64{
		{},
		{42},
		{1, 2, 3, 4, 5},
		{1000, 990, 980, 970},
		{-5, 0, 5, 10, 1000000},
	}
	for _, enc := range []byte{encIntPlain, encIntDelta, encIntDoD} {
		for _, vals := range cases {
			data := encodeInts(vals, enc)
			got, err := decodeInts(data, len(vals), enc)
			if err != nil {
				t.Fatalf("enc %d decode: %v", enc, err)
			}
			if !reflect.DeepEqual(got, vals) && !(len(vals) == 0 && len(got) == 0) {
				t.Fatalf("enc %d: got %v want %v", enc, got, vals)
			}
		}
	}
}

func TestEncodeDecodeStrings(t *testing.T) {
	// Repetitive (low-cardinality) data must pick dictionary encoding.
	vals := make([]string, 4000)
	opts := []string{"approved", "declined", "refunded", "pending"}
	for i := range vals {
		vals[i] = opts[i%len(opts)]
	}
	enc, data, dictSize := encodeStrings(vals)
	if enc != encStrDict || dictSize != 4 {
		t.Fatalf("expected dict encoding, got enc=%d with %d entries", enc, dictSize)
	}
	got, err := decodeStrings(data, len(vals), enc)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, vals) {
		t.Fatal("dictionary round trip failed")
	}
	// NULL-ish / empty strings mixed with repeated values round trip too.
	mixed := []string{"a", "b", "a", "", "abcdefgh", "a", "zzz"}
	encM, dataM, _ := encodeStrings(mixed)
	gotM, err := decodeStrings(dataM, len(mixed), encM)
	if err != nil || !reflect.DeepEqual(gotM, mixed) {
		t.Fatalf("mixed round trip failed: %v", err)
	}
	// High-cardinality data should fall back to plain.
	many := make([]string, 5000)
	for i := range many {
		many[i] = time.UnixMicro(int64(i) * 7919).Format("20060102150405.000000")
	}
	enc2, data2, _ := encodeStrings(many)
	got2, err := decodeStrings(data2, len(many), enc2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got2, many) {
		t.Fatal("high-cardinality round trip failed")
	}
}
