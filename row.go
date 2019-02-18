package hake

import (
	"cloud.google.com/go/spanner"
	gspanner "google.golang.org/genproto/googleapis/spanner/v1"
)

// Columns returns columns of the row as []*spanner.GenericColumnValue.
func Columns(row *spanner.Row) ([]*spanner.GenericColumnValue, error) {
	names := row.ColumnNames()
	cols := make([]*spanner.GenericColumnValue, 0, len(names))
	for _, n := range names {
		var col spanner.GenericColumnValue
		if err := row.ColumnByName(n, &col); err != nil {
			return nil, err
		}
		cols = append(cols, &col)
	}
	return cols, nil
}

// Types returns column types of the row.
func Types(row *spanner.Row) ([]*gspanner.Type, error) {
	cols, err := Columns(row)
	if err != nil {
		return nil, err
	}

	types := make([]*gspanner.Type, len(cols))
	for i, col := range cols {
		types[i] = col.Type
	}

	return types, nil
}
