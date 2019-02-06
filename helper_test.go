package hake_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/civil"
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

func date(t *testing.T, s string) civil.Date {
	t.Helper()
	d, err := civil.ParseDate(s)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	return d
}

func timestamp(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	return tm
}

// R is Row.
type R map[string]interface{}

func row(t *testing.T, r R) *spanner.Row {
	t.Helper()

	if r == nil {
		return nil
	}

	names := make([]string, 0, len(r))
	values := make([]interface{}, 0, len(r))
	for n, v := range r {
		names = append(names, n)
		values = append(values, v)
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
