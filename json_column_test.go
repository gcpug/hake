package hake_test

import (
	"testing"

	"cloud.google.com/go/spanner"
	. "github.com/gcpug/hake"
	structpb "github.com/golang/protobuf/ptypes/struct"
	gspanner "google.golang.org/genproto/googleapis/spanner/v1"
)

func TestColumn_MarshalJSON(t *testing.T) {

	type T struct {
		N interface{}
		S string
	}

	type NT struct {
		T T
	}

	floatWithString := func(s string) *spanner.GenericColumnValue {
		return &spanner.GenericColumnValue{Type: &gspanner.Type{Code: gspanner.TypeCode_FLOAT64}, Value: &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: s}}}
	}

	cases := []struct {
		name string
		col  *spanner.GenericColumnValue
		want string
	}{
		{"null", column(t, nil), toJSON(t, nil)},
		{"int", column(t, 100), toJSON(t, "100")},
		{"big int", column(t, 72057596404714278), toJSON(t, "72057596404714278")},
		{"float", column(t, 10.5), toJSON(t, 10.5)},
		{"float with NaN", floatWithString("NaN"), toJSON(t, nil)},
		{"float with Infinity", floatWithString("Infinity"), toJSON(t, nil)},
		{"float with -Infinity", floatWithString("-Infinity"), toJSON(t, nil)},
		{"string", column(t, "test"), toJSON(t, "test")},
		{"bool", column(t, true), toJSON(t, true)},
		{"struct", column(t, T{N: 100, S: "test"}), toJSON(t, T{N: "100", S: "test"})},
		{"nested struct", column(t, NT{T{N: 100, S: "test"}}), toJSON(t, NT{T{N: "100", S: "test"}})},
		{"timestamp", column(t, timestamp(t, "2002-10-02T10:00:00Z")), toJSON(t, "2002-10-02T10:00:00Z")},
		{"bytes", column(t, []byte("test")), toJSON(t, []byte("test"))},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := toJSON(t, (*JSONColumn)(tt.col))
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}
