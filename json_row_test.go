package hake_test

import (
	"testing"

	"cloud.google.com/go/spanner"
	. "github.com/gcpug/hake"
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
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
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
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
			got := toJSON(t, JSONRows(tt.rows))
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}
