package hake

import (
	"bytes"
	"encoding/json"
	"io"
	"path"

	"cloud.google.com/go/spanner"
	"github.com/tenntenn/jsonschema"
)

// JSONRow is an encodable type of spanner.Row.
type JSONRow spanner.Row

var _ json.Marshaler = (*JSONRow)(nil)

// MarshalJSON implements json.Marshaler
func (r *JSONRow) MarshalJSON() ([]byte, error) {
	row := (*spanner.Row)(r)
	names := row.ColumnNames()
	m := make(map[string]interface{}, len(names))
	for _, n := range names {
		var col spanner.GenericColumnValue
		if err := row.ColumnByName(n, &col); err != nil {
			return nil, err
		}
		m[n] = (*JSONColumn)(&col)
	}
	return json.Marshal(m)
}

// JSONObject is interface of JSON object.
type JSONObject interface {
	Set(key string, value interface{})
	Get(key string) (interface{}, bool)
	Ref() string
}

type mapJSONObject struct {
	m   map[string]interface{}
	ref string
}

func (o *mapJSONObject) Set(key string, value interface{}) {
	o.m[key] = value
}

func (o *mapJSONObject) Get(key string) (value interface{}, ok bool) {
	value, ok = o.m[key]
	return
}

func (o *mapJSONObject) Ref() string {
	return o.ref
}

// Schema writes JSON Schema of the row to writer w.
func (r *JSONRow) JSONSchema(w io.Writer, options ...jsonschema.Option) error {
	type colSchema struct {
		Name   string
		Schema string
	}
	names := (*spanner.Row)(r).ColumnNames()
	cols := make([]colSchema, len(names))

	var buf bytes.Buffer
	for i := range names {
		var col spanner.GenericColumnValue
		if err := (*spanner.Row)(r).ColumnByName(names[i], &col); err != nil {
			return err
		}

		o := &mapJSONObject{
			m:   map[string]interface{}{},
			ref: path.Join("#/properties", names[i]),
		}

		opts := make([]jsonschema.Option, len(options)+1)
		copy(opts, options)
		opts[len(opts)-1] = jsonschema.ByReference(o.Ref(), jsonschema.PropertyOrder(i))

		if err := (*JSONColumn)(&col).schema(o, col.Type, opts...); err != nil {
			return err
		}

		if err := json.NewEncoder(&buf).Encode(o.m); err != nil {
			return err
		}

		cols[i] = colSchema{
			Name:   names[i],
			Schema: buf.String(),
		}

		buf.Reset()
	}

	if err := jsonSchemaTmpl.Execute(&buf, cols); err != nil {
		return err
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, buf.Bytes()); err != nil {
		return err
	}

	if _, err := io.Copy(w, &compact); err != nil {
		return err
	}

	return nil
}

// JSONRows is an encodable type of []*spanner.Row.
type JSONRows []*spanner.Row

var _ json.Marshaler = (JSONRows)(nil)

// At returns ith row as *JSONRow.
func (rs JSONRows) At(i int) *JSONRow {
	return (*JSONRow)(rs[i])
}

func (rs JSONRows) toJSONRowSlice() []*JSONRow {
	if rs == nil {
		return nil
	}

	rows := make([]*JSONRow, len(rs))
	for i := range rs {
		rows[i] = rs.At(i)
	}
	return rows
}

// MarshalJSON implements json.Marshaler
func (rs JSONRows) MarshalJSON() ([]byte, error) {
	return json.Marshal(rs.toJSONRowSlice())
}

// Schema writes JSON Schema of the rows to writer w.
func (rs JSONRows) JSONSchema(w io.Writer, options ...jsonschema.Option) error {
	return jsonschema.Generate(w, rs, options...)
}
