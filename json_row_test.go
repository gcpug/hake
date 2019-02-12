package hake_test

import (
	"bytes"
	"testing"

	"cloud.google.com/go/spanner"
	. "github.com/gcpug/hake"
	"github.com/xeipuuv/gojsonschema"
)

func TestJSONRow_MarshalJSON(t *testing.T) {

	cases := []struct {
		name string
		row  *spanner.Row
		want string
	}{
		{"null", row(t, nil), toJSON(t, nil)},
		{"empty", row(t, R{}), toJSON(t, R{})},
		{"single", row(t, R{"col1": 100}), toJSON(t, R{"col1": 100})},
		{"multiple", row(t, R{"col1": 100, "col2": 10.5}), toJSON(t, R{"col1": 100, "col2": 10.5})},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := toJSON(t, (*JSONRow)(tt.row))
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}

func TestRows(t *testing.T) {

	cases := []struct {
		name string
		rows []*spanner.Row
		want string
	}{
		{"null", rows(t, nil), toJSON(t, nil)},
		{"empties", rows(t, []R{{}, {}}), toJSON(t, []R{{}, {}})},
		{"singles", rows(t, []R{{"col1": 100}, {"col2": 10.5}}), toJSON(t, []R{{"col1": 100}, {"col2": 10.5}})},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := toJSON(t, JSONRows(tt.rows))
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}

func TestJSONRow_Schema(t *testing.T) {

	type T struct {
		N int
		S string
	}

	type NT struct {
		T T
	}

	cases := []struct {
		name  string
		row   *spanner.Row
		isErr bool
	}{
		{"int", row(t, R{"col1": 100}), false},
		{"int string", row(t, R{"col1": 100, "col2": "string"}), false},
		{"nested struct", row(t, R{"col1": 100, "col2": T{N: 100, S: ""}}), false},
		{"timestamp", row(t, R{"col1": 100, "col2": timestamp(t, "2002-10-02T10:00:00Z")}), false},
		{"bytes", row(t, R{"col1": []byte("test")}), false},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var got bytes.Buffer
			err := (*JSONRow)(tt.row).Schema(&got)
			switch {
			case tt.isErr && err == nil:
				t.Errorf("expected error does not occur")
			case !tt.isErr && err != nil:
				t.Errorf("unexpected error %v", err)
			}

			l := gojsonschema.NewStringLoader(got.String())
			s, err := gojsonschema.NewSchema(l)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			rowJSON := toJSON(t, (*JSONRow)(tt.row))
			r, err := s.Validate(gojsonschema.NewStringLoader(rowJSON))
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if !r.Valid() {
				t.Errorf("invalid JSON Schema: %s", got.String())
			}
		})
	}
}
