package model

import (
	"fmt"
	"time"
)

// Query is a unified hot+cold query request. All filters are optional except
// the time range.
type Query struct {
	From       time.Time `json:"from"`
	To         time.Time `json:"to"`
	MerchantID *int64    `json:"merchant_id,omitempty"`
	Status     string    `json:"status,omitempty"`
	Channel    string    `json:"channel,omitempty"`
	Region     string    `json:"region,omitempty"`
	Currency   string    `json:"currency,omitempty"`
	TxnNo      string    `json:"txn_no,omitempty"`
	MinAmount  *int64    `json:"min_amount,omitempty"`
	MaxAmount  *int64    `json:"max_amount,omitempty"`
	MaxID      *int64    `json:"max_id,omitempty"`
	Limit      int       `json:"limit,omitempty"`
	Cursor     *Cursor   `json:"cursor,omitempty"`
}

// Cursor is an opaque-ish pagination token: created_at + id.
type Cursor struct {
	Time time.Time `json:"t"`
	ID   int64     `json:"id"`
}

func (c *Cursor) Key() Key { return Key{Time: c.Time, ID: c.ID} }

// Matches reports whether a row satisfies the non-time predicates. The time
// range is handled by the access paths (source SQL / cold block pruning).
func (q Query) Matches(row Row) bool {
	if q.MerchantID != nil && row[2].I != *q.MerchantID {
		return false
	}
	if q.Status != "" && row[6].S != q.Status {
		return false
	}
	if q.Channel != "" && row[7].S != q.Channel {
		return false
	}
	if q.Region != "" && row[8].S != q.Region {
		return false
	}
	if q.Currency != "" && row[5].S != q.Currency {
		return false
	}
	if q.TxnNo != "" && row[1].S != q.TxnNo {
		return false
	}
	if q.MaxID != nil && row[0].I > *q.MaxID {
		return false
	}
	if q.MinAmount != nil && row[4].I < *q.MinAmount {
		return false
	}
	if q.MaxAmount != nil && row[4].I > *q.MaxAmount {
		return false
	}
	t := time.UnixMicro(row[10].I)
	if t.Before(q.From) || t.After(q.To) {
		return false
	}
	return true
}

// Validate checks the query bounds.
func (q Query) Validate() error {
	if q.From.IsZero() || q.To.IsZero() {
		return fmt.Errorf("from and to are required")
	}
	if q.To.Before(q.From) {
		return fmt.Errorf("to must not be before from")
	}
	if q.To.Sub(q.From) > 20*366*24*time.Hour {
		return fmt.Errorf("time range too large")
	}
	return nil
}

// Hold scopes.
const (
	HoldTxn       = "txn"
	HoldTxnRange  = "txn_range"
	HoldMerchant  = "merchant"
	HoldTimeRange = "time_range"
)

// Hold is a legal preservation order. Rows matched by an active hold are never
// removed from the cold store or the source database, regardless of policy.
type Hold struct {
	ID          int64      `json:"id"`
	CaseNo      string     `json:"case_no"`
	Reason      string     `json:"reason"`
	Scope       string     `json:"scope"`
	TargetValue string     `json:"target_value"`
	TargetFrom  int64      `json:"target_from"`
	TargetTo    int64      `json:"target_to"`
	TimeFrom    *time.Time `json:"time_from,omitempty"`
	TimeTo      *time.Time `json:"time_to,omitempty"`
	Active      bool       `json:"active"`
	CreatedBy   string     `json:"created_by"`
	CreatedAt   time.Time  `json:"created_at"`
	ReleasedAt  *time.Time `json:"released_at,omitempty"`
}

// HoldSet is an in-memory matcher used by the cold-store rewriter.
type HoldSet []Hold

// Matches reports whether the row falls under any active hold.
func (hs HoldSet) Matches(row Row) bool {
	return hs.MatchesFields(row[0].I, row[1].S, row[2].I, time.UnixMicro(row[10].I))
}

// MatchesFields evaluates holds from primitive fields, allowing the source
// adapter to apply holds without materializing a full row.
func (hs HoldSet) MatchesFields(id int64, txnNo string, merchantID int64, t time.Time) bool {
	for _, h := range hs {
		if !h.Active {
			continue
		}
		switch h.Scope {
		case HoldTxn:
			if txnNo == h.TargetValue {
				return true
			}
		case HoldTxnRange:
			if id >= h.TargetFrom && id <= h.TargetTo {
				return true
			}
		case HoldMerchant:
			if fmt.Sprintf("%d", merchantID) == h.TargetValue {
				return true
			}
		case HoldTimeRange:
			inFrom := h.TimeFrom == nil || !t.Before(*h.TimeFrom)
			inTo := h.TimeTo == nil || t.Before(*h.TimeTo)
			if inFrom && inTo {
				return true
			}
		}
	}
	return false
}
