package hake

import (
	"fmt"
	"strconv"

	"cloud.google.com/go/spanner"
	structpb "github.com/golang/protobuf/ptypes/struct"
	gspanner "google.golang.org/genproto/googleapis/spanner/v1"
)

// RecordWriter writes recod.
type RecordWriter interface {
	Write(record []string) error
}

// WriteHeaderTo is writes the row to the writer w as a header.
func WriteHeaderTo(w RecordWriter, row *spanner.Row) error {
	cw := NewWriter(w)
	cw.WriteHeader(row)
	return cw.Error()
}

// WriteHeaderTo is writes values of the row to the writer w.
func WriteTo(w RecordWriter, row *spanner.Row) error {
	cw := NewWriter(w)
	cw.Write(row)
	return cw.Error()
}

// Writer is writes *spanner.Row to Recordwriter.
type Writer struct {
	err error
	w   RecordWriter
}

// NewWriter creates a Writer.
func NewWriter(w RecordWriter) *Writer {
	return &Writer{w: w}
}

// Error returns occured error.
func (w *Writer) Error() error {
	return w.err
}

// WriteHeader writes a header.
func (w *Writer) WriteHeader(row *spanner.Row) {
	if w.err != nil || row == nil {
		return
	}

	if err := w.validateTypes(row); err != nil {
		w.err = err
		return
	}

	if err := w.w.Write(row.ColumnNames()); err != nil {
		w.err = err
	}
}

func (w *Writer) validateTypes(row *spanner.Row) error {

	types, err := Types(row)
	if err != nil {
		return err
	}

	for i := range types {
		if err := w.validateType(types[i]); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) validateType(t *gspanner.Type) error {
	switch t.Code {
	case gspanner.TypeCode_INT64, gspanner.TypeCode_FLOAT64,
		gspanner.TypeCode_STRING, gspanner.TypeCode_BOOL,
		gspanner.TypeCode_DATE, gspanner.TypeCode_TIMESTAMP,
		gspanner.TypeCode_BYTES:
		return nil
	}
	return fmt.Errorf("unsupported type: %v", t)
}

// Write writes a row.
func (w *Writer) Write(row *spanner.Row) {
	if w.err != nil || row == nil {
		return
	}

	if err := w.validateTypes(row); err != nil {
		w.err = err
		return
	}

	cols, err := Columns(row)
	if err != nil {
		w.err = err
		return
	}

	record := make([]string, len(cols))
	for i := range cols {
		v, err := w.value(cols[i])
		if err != nil {
			w.err = err
			return
		}
		record[i] = v
	}

	if err := w.w.Write(record); err != nil {
		w.err = err
	}
}

func (w *Writer) value(c *spanner.GenericColumnValue) (string, error) {
	if _, isNull := c.Value.Kind.(*structpb.Value_NullValue); isNull {
		return "", nil
	}

	switch c.Type.Code {
	case gspanner.TypeCode_INT64, gspanner.TypeCode_STRING,
		gspanner.TypeCode_DATE, gspanner.TypeCode_TIMESTAMP,
		gspanner.TypeCode_BYTES:
		return c.Value.GetStringValue(), nil
	case gspanner.TypeCode_FLOAT64:
		return strconv.FormatFloat(c.Value.GetNumberValue(), 'f', -1, 64), nil
	case gspanner.TypeCode_BOOL:
		return strconv.FormatBool(c.Value.GetBoolValue()), nil
	}
	return "", fmt.Errorf("unsupport type: type:%v value:%T", c.Type, c.Value.Kind)
}
