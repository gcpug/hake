package hake_test

import (
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/gcpug/hake"
)

type recordWriter struct {
	record []string
	err    error
}

func (w *recordWriter) Write(record []string) error {
	if w.err != nil {
		return w.err
	}
	w.record = record
	return nil
}

func TestWriter_Write(t *testing.T) {

	cases := []struct {
		name   string
		row    *spanner.Row
		want   []string
		hasErr bool
	}{
		{"null", row(t, nil), nil, false},
		{"empty", row(t, R{}), []string{}, false},
		{"single", row(t, R{"col1", 100}), []string{"100"}, false},
		{"multiple", row(t, R{"col1", 100, "col2", 10.5}), []string{"100", "10.5"}, false},
		{"duplicate column name", row(t, R{"col", 100, "col", 10.5}), []string{"100", "10.5"}, false},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var rw recordWriter
			if tt.hasErr {
				rw.err = errors.New("error")
			}

			w := hake.NewWriter(&rw, false)
			switch err := w.Write(tt.row); {
			case tt.hasErr && err == nil:
				t.Errorf("expected error does not occur")
			case !tt.hasErr && err != nil:
				t.Errorf("unexpected error: %v", err)
			}

			got := rw.record
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("want %#v but got %#v", tt.want, got)
			}
		})
	}
}
