package hake_test

import (
	"testing"

	"cloud.google.com/go/spanner"
	. "github.com/sinmetal/hake"
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
		{"null", column(t, nil), toJSON(t, nil)},
		{"int", column(t, 100), toJSON(t, 100)},
		{"float", column(t, 10.5), toJSON(t, 10.5)},
		{"string", column(t, "test"), toJSON(t, "test")},
		{"bool", column(t, true), toJSON(t, true)},
		{"struct", column(t, T{N: 100, S: "test"}), toJSON(t, T{N: 100, S: "test"})},
		{"nested struct", column(t, NT{T{N: 100, S: "test"}}), toJSON(t, NT{T{N: 100, S: "test"}})},
		{"timestamp", column(t, timestamp(t, "2002-10-02T10:00:00Z")), toJSON(t, "2002-10-02T10:00:00Z")},
		{"bytes", column(t, []byte("test")), toJSON(t, []byte("test"))},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
			got := toJSON(t, (*JSONColumn)(tt.col))
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}
