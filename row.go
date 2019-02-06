package hake

import (
	"encoding/json"

	"cloud.google.com/go/spanner"
)

// Row is an encodable type of spanner.Row.
type Row spanner.Row

var _ json.Marshaler = (*Row)(nil)

// MarshalJSON implements json.Marshaler
func (r *Row) MarshalJSON() ([]byte, error) {
	row := (*spanner.Row)(r)
	names := row.ColumnNames()
	m := make(map[string]interface{}, len(names))
	for _, n := range names {
		var col spanner.GenericColumnValue
		if err := row.ColumnByName(n, &col); err != nil {
			return nil, err
		}
		m[n] = (*Column)(&col)
	}
	return json.Marshal(m)
}

// Rows convert []*spanner.Row to []*Row.
func Rows(rows []*spanner.Row) []*Row {
	if rows == nil {
		return nil
	}

	rs := make([]*Row, len(rows))
	for i := range rows {
		rs[i] = (*Row)(rows[i])
	}

	return rs
}
