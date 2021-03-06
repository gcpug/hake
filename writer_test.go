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

type multiRecordWriter struct {
	records [][]string
	err     error
}

func (w *multiRecordWriter) Write(record []string) error {
	if w.err != nil {
		return w.err
	}
	w.records = append(w.records, record)
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

func TestWriter_Write_MultiRow(t *testing.T) {
	var nilArray [][]string
	cases := []struct {
		name   string
		rows   []*spanner.Row
		header bool
		want   [][]string
		hasErr bool
	}{
		{"null", rows(t, nil), false, nil, false},
		{"empty", rows(t, []R{}), true, nilArray, false}, // Rowがないとカラム名が分からないので、header=trueでも、何も出力されない
		{"empty no header", rows(t, []R{}), false, nilArray, false},
		{"single", rows(t, []R{R{"col1", 100}, R{"col1", 111}}), true, [][]string{{"col1"}, {"100"}, {"111"}}, false},
		{"single no header", rows(t, []R{R{"col1", 100}, R{"col1", 111}}), false, [][]string{{"100"}, {"111"}}, false},
		{"multiple", rows(t, []R{R{"col1", 100, "col2", 10.5}, R{"col1", 111, "col2", 10.8}}), true, [][]string{{"col1", "col2"}, {"100", "10.5"}, {"111", "10.8"}}, false},
		{"multiple no header", rows(t, []R{R{"col1", 100, "col2", 10.5}, R{"col1", 111, "col2", 10.8}}), false, [][]string{{"100", "10.5"}, {"111", "10.8"}}, false},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var rw multiRecordWriter
			if tt.hasErr {
				rw.err = errors.New("error")
			}

			w := hake.NewWriter(&rw, tt.header)
			for _, row := range tt.rows {
				switch err := w.Write(row); {
				case tt.hasErr && err == nil:
					t.Errorf("expected error does not occur")
				case !tt.hasErr && err != nil:
					t.Errorf("unexpected error: %v", err)
				}
			}

			got := rw.records
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("want %#v but got %#v", tt.want, got)
			}
		})
	}
}
