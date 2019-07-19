package hake

import (
	"encoding/json"
	"fmt"
	"path"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/tenntenn/jsonschema"
	gspanner "google.golang.org/genproto/googleapis/spanner/v1"
)

// JSONColumn is an encodable type of spanner.GenericColumnValue.
type JSONColumn spanner.GenericColumnValue

var _ json.Marshaler = (*JSONColumn)(nil)

// MarshalJSON implements json.Marshaler
func (c *JSONColumn) MarshalJSON() ([]byte, error) {
	v, err := c.marshal(c.Type, c.Value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func (c *JSONColumn) marshal(t *gspanner.Type, v *structpb.Value) (interface{}, error) {
	if _, isNull := v.Kind.(*structpb.Value_NullValue); isNull {
		return nil, nil
	}

	// See: https://godoc.org/google.golang.org/genproto/googleapis/spanner/v1#TypeCode
	switch t.Code {
	case gspanner.TypeCode_INT64:
		// JavaScript's integral part is 53bit. see Number.MAX_SAFE_INTEGER.
		return v.GetStringValue(), nil
	case gspanner.TypeCode_FLOAT64:
		s := v.GetStringValue()
		switch s {
		case "NaN", "Infinity", "-Infinity":
			// NaN, Infinity, -Infinity will be null on JSON with JavaScript.
			// http://www.ecma-international.org/ecma-262/10.0/index.html#sec-serializejsonproperty
			return nil, nil
		}
		// golang:     https://golang.org/ref/spec#Numeric_types
		// 		       float64     the set of all IEEE-754 64-bit floating-point numbers
		// ECMAScript: http://www.ecma-international.org/ecma-262/10.0/index.html#sec-ecmascript-language-types-number-type
		//             representing the double-precision 64-bit format IEEE 754-2008 values as specified in the IEEE Standard for Binary Floating-Point Arithmetic
		return v.GetNumberValue(), nil
	case gspanner.TypeCode_STRING:
		return v.GetStringValue(), nil
	case gspanner.TypeCode_BOOL:
		return v.GetBoolValue(), nil
	case gspanner.TypeCode_STRUCT:
		return c.marshalStruct(t.GetStructType(), v.GetListValue())
	case gspanner.TypeCode_ARRAY:
		return c.marshalList(t.GetArrayElementType(), v.GetListValue())
	case gspanner.TypeCode_DATE:
		d, err := civil.ParseDate(v.GetStringValue())
		if err != nil {
			return nil, err
		}
		return d, nil
	case gspanner.TypeCode_TIMESTAMP:
		return time.Parse(time.RFC3339, v.GetStringValue())
	case gspanner.TypeCode_BYTES:
		return v.GetStringValue(), nil
	}
	return nil, fmt.Errorf("unsupport type: type:%v value:%T", t, v.Kind)
}

func (c *JSONColumn) marshalStruct(t *gspanner.StructType, fs *structpb.ListValue) (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(fs.Values))

	for i := range fs.Values {
		v, err := c.marshal(t.Fields[i].Type, fs.Values[i])
		if err != nil {
			return nil, err
		}
		m[t.Fields[i].Name] = v
	}

	return m, nil
}

func (c *JSONColumn) marshalList(t *gspanner.Type, l *structpb.ListValue) ([]interface{}, error) {
	vs := make([]interface{}, len(l.Values))

	for i := range l.Values {
		v, err := c.marshal(t, l.Values[i])
		if err != nil {
			return nil, err
		}
		vs[i] = v
	}

	return vs, nil
}

func (c *JSONColumn) schema(o JSONObject, t *gspanner.Type, options ...jsonschema.Option) error {

	switch t.Code {
	default:
		return fmt.Errorf("unsupport type: type:%v", t)
	case gspanner.TypeCode_INT64:
		o.Set("type", "string")
		o.Set("format", "int64")
	case gspanner.TypeCode_FLOAT64:
		o.Set("type", "number")
	case gspanner.TypeCode_STRING:
		o.Set("type", "string")
	case gspanner.TypeCode_BOOL:
		o.Set("type", "boolean")
	case gspanner.TypeCode_DATE:
		o.Set("type", "string")
		o.Set("format", "date")
	case gspanner.TypeCode_TIMESTAMP:
		o.Set("type", "string")
		o.Set("format", "datetime")
	case gspanner.TypeCode_BYTES:
		o.Set("type", "string")
		o.Set("format", "textarea")
	case gspanner.TypeCode_STRUCT:
		if err := c.schemaStruct(o, t.GetStructType()); err != nil {
			return err
		}
	case gspanner.TypeCode_ARRAY:
		if err := c.schemaArray(o, t.GetArrayElementType()); err != nil {
			return err
		}
	}

	for i := range options {
		var err error
		o, err = (options[i])(o)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *JSONColumn) schemaStruct(parent JSONObject, t *gspanner.StructType, options ...jsonschema.Option) error {

	required := make([]string, len(t.Fields))
	properties := make(map[string]interface{}, len(t.Fields))

	for i, f := range t.Fields {

		required[i] = f.Name

		o := &mapJSONObject{
			m:   map[string]interface{}{},
			ref: path.Join(parent.Ref(), "properties", f.Name),
		}

		opts := make([]jsonschema.Option, len(options)+1)
		copy(opts, options)
		opts[len(opts)-1] = jsonschema.ByReference(o.Ref(), jsonschema.PropertyOrder(i))

		if err := c.schema(o, f.Type, opts...); err != nil {
			return err
		}

		properties[f.Name] = o.m
	}

	parent.Set("type", "object")
	parent.Set("required", required)
	parent.Set("properties", properties)

	return nil
}

func (c *JSONColumn) schemaArray(parent JSONObject, t *gspanner.Type, options ...jsonschema.Option) error {

	o := &mapJSONObject{
		m:   map[string]interface{}{},
		ref: path.Join(parent.Ref(), "items"),
	}

	if err := c.schema(o, t, options...); err != nil {
		return err
	}

	parent.Set("type", "array")
	parent.Set("format", "table")
	parent.Set("items", o.m)

	return nil
}
