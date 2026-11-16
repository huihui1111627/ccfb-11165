// Package model contains the logical transaction schema shared by the source
// database, the cold columnar files, the unified query layer and the verifier.
package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// Kind is the physical kind of a column inside the cold store.
type Kind uint8

const (
	KindInt64 Kind = iota + 1
	KindString
	KindTimestamp // stored as int64 unix microseconds (UTC)
)

func (k Kind) String() string {
	switch k {
	case KindInt64:
		return "int64"
	case KindString:
		return "string"
	case KindTimestamp:
		return "timestamp"
	default:
		return fmt.Sprintf("kind(%d)", k)
	}
}

func (k Kind) MarshalJSON() ([]byte, error) { return json.Marshal(k.String()) }

func (k *Kind) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch s {
	case "int64":
		*k = KindInt64
	case "string":
		*k = KindString
	case "timestamp":
		*k = KindTimestamp
	default:
		return fmt.Errorf("unknown column kind %q", s)
	}
	return nil
}

// Column describes one column.
type Column struct {
	Name     string `json:"name"`
	Kind     Kind   `json:"kind"`
	Nullable bool   `json:"nullable"`
}

// Schema describes a table. Only the columns listed here are archived.
type Schema struct {
	Table     string   `json:"table"`
	TimeField string   `json:"time_field"`
	PKField   string   `json:"pk_field"`
	Columns   []Column `json:"columns"`
	index     map[string]int
}

func NewSchema(table, timeField, pkField string, cols []Column) *Schema {
	s := &Schema{Table: table, TimeField: timeField, PKField: pkField, Columns: cols}
	s.index = make(map[string]int, len(cols))
	for i, c := range cols {
		s.index[c.Name] = i
	}
	return s
}

// Index returns the position of a column, or -1.
func (s *Schema) Index(name string) int {
	if s.index == nil {
		s.index = make(map[string]int, len(s.Columns))
		for i, c := range s.Columns {
			s.index[c.Name] = i
		}
	}
	if i, ok := s.index[name]; ok {
		return i
	}
	return -1
}

// Value is one cell. Exactly one of I or S is meaningful depending on the
// column kind; Null marks a SQL NULL.
type Value struct {
	I    int64
	S    string
	Null bool
}

// Row is a full logical row in schema order.
type Row []Value

// Record is a row plus the source-side concurrency token (updated_at).
type Record struct {
	Row       Row
	UpdatedAt time.Time
}

// Transactions is the demo payment transaction table.
var Transactions = NewSchema("transactions", "created_at", "id", []Column{
	{Name: "id", Kind: KindInt64},
	{Name: "txn_no", Kind: KindString},
	{Name: "merchant_id", Kind: KindInt64},
	{Name: "card_last4", Kind: KindString},
	{Name: "amount_cents", Kind: KindInt64},
	{Name: "currency", Kind: KindString},
	{Name: "status", Kind: KindString},
	{Name: "channel", Kind: KindString},
	{Name: "region", Kind: KindString},
	{Name: "risk_score", Kind: KindInt64},
	{Name: "created_at", Kind: KindTimestamp},
	{Name: "memo", Kind: KindString, Nullable: true},
})

// TransactionRow is the canonical wire representation returned by the unified
// query API. CreatedAt is pre-formatted so the direct-source verifier and the
// API produce byte-identical JSON.
type TransactionRow struct {
	ID          int64   `json:"id"`
	TxnNo       string  `json:"txn_no"`
	MerchantID  int64   `json:"merchant_id"`
	CardLast4   string  `json:"card_last4"`
	AmountCents int64   `json:"amount_cents"`
	Currency    string  `json:"currency"`
	Status      string  `json:"status"`
	Channel     string  `json:"channel"`
	Region      string  `json:"region"`
	RiskScore   int64   `json:"risk_score"`
	CreatedAt   string  `json:"created_at"`
	Memo        *string `json:"memo"`
}

// TimeLayout is the canonical timestamp representation used on the wire.
const TimeLayout = "2006-01-02T15:04:05.000000Z07:00"

func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeLayout)
}

func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// ToRow converts a full transaction row into a canonical Row.
func (r TransactionRow) ToRow() Row {
	created, _ := ParseTime(r.CreatedAt)
	row := make(Row, len(Transactions.Columns))
	row[0] = Value{I: r.ID}
	row[1] = Value{S: r.TxnNo}
	row[2] = Value{I: r.MerchantID}
	row[3] = Value{S: r.CardLast4}
	row[4] = Value{I: r.AmountCents}
	row[5] = Value{S: r.Currency}
	row[6] = Value{S: r.Status}
	row[7] = Value{S: r.Channel}
	row[8] = Value{S: r.Region}
	row[9] = Value{I: r.RiskScore}
	row[10] = Value{I: created.UnixMicro()}
	if r.Memo != nil {
		row[11] = Value{S: *r.Memo}
	} else {
		row[11] = Value{Null: true}
	}
	return row
}

// RowToTransaction converts a Row into its canonical wire form.
func RowToTransaction(row Row) TransactionRow {
	tr := TransactionRow{
		ID:          row[0].I,
		TxnNo:       row[1].S,
		MerchantID:  row[2].I,
		CardLast4:   row[3].S,
		AmountCents: row[4].I,
		Currency:    row[5].S,
		Status:      row[6].S,
		Channel:     row[7].S,
		Region:      row[8].S,
		RiskScore:   row[9].I,
		CreatedAt:   FormatTime(time.UnixMicro(row[10].I)),
	}
	if !row[11].Null {
		memo := row[11].S
		tr.Memo = &memo
	}
	return tr
}

// Key is the sort key used by the unified query: (created_at, id) descending.
type Key struct {
	Time time.Time
	ID   int64
}

func (k Key) Before(o Key) bool {
	if k.Time.Equal(o.Time) {
		return k.ID < o.ID
	}
	return k.Time.Before(o.Time)
}
