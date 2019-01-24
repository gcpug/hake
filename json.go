package sgcvj

import (
	"cloud.google.com/go/spanner"
	"fmt"
)

type ColumnValue spanner.GenericColumnValue

func (v *ColumnValue) UnmarshalJSON(b []byte) error {
	return nil
}

func (v *ColumnValue) MarshalJSON() ([]byte, error) {
	fmt.Printf("ColumnValue.MarshalJSON() %+v\n", v)
	return nil, nil
}
