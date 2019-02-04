package hake

import (
	"encoding/json"

	"cloud.google.com/go/spanner"
)

// JSONRow is an encodable type of spanner.Row.
type JSONRow spanner.Row

var _ json.Marshaler = (*JSONRow)(nil)

// MarshalJSON implements json.Marshaler
func (r *JSONRow) MarshalJSON() ([]byte, error) {
	row := (*spanner.Row)(r)
	names := row.ColumnNames()
	m := make(map[string]interface{}, len(names))
	for _, n := range names {
		var col spanner.GenericColumnValue
		if err := row.ColumnByName(n, &col); err != nil {
			return nil, err
		}
		m[n] = (*JSONColumn)(&col)
	}
	return json.Marshal(m)
}

// Rows convert []*spanner.Row to []*Row.
func JSONRows(rows []*spanner.Row) []*JSONRow {
	if rows == nil {
		return nil
	}

	rs := make([]*JSONRow, len(rows))
	for i := range rows {
		rs[i] = (*JSONRow)(rows[i])
	}

	return rs
}
