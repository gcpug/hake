package hake_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
)

func toJSON(t *testing.T, v interface{}) string {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal("unexpected error", err)
	}
	return buf.String()
}

func column(t *testing.T, v interface{}) *spanner.GenericColumnValue {
	t.Helper()

	if v == nil {
		return nil
	}

	row, err := spanner.NewRow([]string{"col"}, []interface{}{v})
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	var col spanner.GenericColumnValue
	if err := row.Column(0, &col); err != nil {
		t.Fatal("unexpected error", err)
	}

	return &col
}

// R is Row.
type R []interface{}

func (r R) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	for i := 0; i < len(r)-1; i += 2 {
		m[fmt.Sprint(r[i])] = r[i+1]
	}
	return json.Marshal(m)
}

func row(t *testing.T, r R) *spanner.Row {
	t.Helper()

	if r == nil {
		return nil
	}

	names := make([]string, 0, len(r))
	values := make([]interface{}, 0, len(r))
	for i := 0; i < len(r)-1; i += 2 {
		names = append(names, fmt.Sprint(r[i]))
		values = append(values, r[i+1])
	}

	rw, err := spanner.NewRow(names, values)
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	return rw
}

func rows(t *testing.T, rs []R) []*spanner.Row {
	t.Helper()

	if rs == nil {
		return nil
	}

	rows := make([]*spanner.Row, len(rs))
	for i := range rs {
		rows[i] = row(t, rs[i])
	}

	return rows
}
