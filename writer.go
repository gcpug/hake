package hake

import (
	"encoding/csv"
	"fmt"
	"strconv"

	"cloud.google.com/go/spanner"
	structpb "github.com/golang/protobuf/ptypes/struct"
	gspanner "google.golang.org/genproto/googleapis/spanner/v1"
)

// RecordWriter writes a record.
// csv.Writer implements RecordWriter.
type RecordWriter interface {
	Write(record []string) error
}

var _ RecordWriter = (*csv.Writer)(nil)

// Writer is writes *spanner.Row to Recordwriter.
type Writer struct {
	w             RecordWriter
	header        bool
	writtenHeader bool
}

// NewWriter creates a Writer.
func NewWriter(w RecordWriter, header bool) *Writer {
	return &Writer{
		w:      w,
		header: header,
	}
}

// Write writes a row of spanner to RecordWriter.
// If it is first time to write, Write writes also a header before writing a row.
// When second argument of NewWriter is false, the header would be omit.
//
// Example with csv.Writer:
//	func query(ctx context.Context, w io.Writer, client *spanner.Client) error {
//		stmt := spanner.Statement{SQL: `SELECT * FROM mytable`}
//		iter := client.Single().Query(ctx, stmt)
//		defer iter.Stop()
//		cw := csv.NewWriter(w)
//		hw := hake.NewWriter(cw, true)
//		for {
//			row, err := iter.Next()
//			switch {
//			case err == iterator.Done:
//				return nil
//			case err != nil:
//				return err
//			}
//
//			if err := hw.Write(row); err != nil {
//				return err
//			}
//			cw.Flush()
//		}
//	}
func (w *Writer) Write(row *spanner.Row) error {
	if row == nil {
		return nil
	}

	if err := w.validateTypes(row); err != nil {
		return err
	}

	if w.header && !w.writtenHeader {
		w.writtenHeader = true
		if err := w.writeHeader(row); err != nil {
			return err
		}
	}

	cols, err := Columns(row)
	if err != nil {
		return err
	}

	record := make([]string, len(cols))
	for i := range cols {
		v, err := w.value(cols[i])
		if err != nil {
			return err
		}
		record[i] = v
	}

	if err := w.w.Write(record); err != nil {
		return err
	}

	return nil
}

func (w *Writer) writeHeader(row *spanner.Row) error {
	return w.w.Write(row.ColumnNames())
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
