package sgcvj_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/sgcvj"
)

func TestColumn_MarshalJSON(t *testing.T) {

	type T struct {
		N int
		S string
	}

	type NT struct {
		T T
	}

	cases := []struct {
		name string
		col  *spanner.GenericColumnValue
		want string
	}{
		{"int", column(t, 100), toJSON(t, 100)},
		{"float", column(t, 10.5), toJSON(t, 10.5)},
		{"string", column(t, "test"), toJSON(t, "test")},
		{"bool", column(t, true), toJSON(t, true)},
		{"struct", column(t, T{N: 100, S: "test"}), toJSON(t, T{N: 100, S: "test"})},
		{"nested struct", column(t, NT{T{N: 100, S: "test"}}), toJSON(t, NT{T{N: 100, S: "test"}})},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
			got := toJSON(t, (*sgcvj.Column)(tt.col))
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}

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
