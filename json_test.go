package sgcvj

import (
	"encoding/json"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
)

func TestJsonMarshal(t *testing.T) {
	type H struct {
		Text   string
		Number int
	}

	columns := []string{"Message", "Array", "Struct"}
	a := []string{"v1", "v2"}
	h := H{Text: "hello struct", Number: 101}
	row, err := spanner.NewRow(columns, []interface{}{"hello world", a, h})
	if err != nil {
		t.Fatal(err)
	}

	m := map[string]ColumnValue{}
	for _, column := range columns {
		var v spanner.GenericColumnValue
		if err := row.ColumnByName(column, &v); err != nil {
			t.Fatalf("failed row.ColumnByName. column=%s, err=%+v", column, err)
		}
		m[column] = ColumnValue(v)
	}

	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("failed json.Marshal. err=%+v", err)
	}
	fmt.Printf("%s\n", string(b))

	type Response struct {
		Message string
		Array   []string
		H       H
	}
	var res Response
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatalf("failed json.Unmarshal. err=%+v", err)
	}

	if e, g := "hello world", res.Message; e != g {
		t.Fatalf("Message expected %v, got %v", e, g)
	}
	if e, g := a, res.Array; cmp.Equal(e, g) == false {
		t.Fatalf("Array expected %v, got %v", e, g)
	}
	if e, g := h, res.H; cmp.Equal(e, g) == false {
		t.Fatalf("Struct expected %v, got %v", e, g)
	}
}
