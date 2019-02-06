package hake

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	structpb "github.com/golang/protobuf/ptypes/struct"
	gspanner "google.golang.org/genproto/googleapis/spanner/v1"
)

// Column is an encodable type of spanner.GenericColumnValue.
type Column spanner.GenericColumnValue

var _ json.Marshaler = (*Column)(nil)

// MarshalJSON implements json.Marshaler
func (c *Column) MarshalJSON() ([]byte, error) {
	v, err := c.marshal(c.Type, c.Value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func (c *Column) marshal(t *gspanner.Type, v *structpb.Value) (interface{}, error) {
	if _, isNull := v.Kind.(*structpb.Value_NullValue); isNull {
		return nil, nil
	}

	// See: https://godoc.org/google.golang.org/genproto/googleapis/spanner/v1#TypeCode
	switch t.Code {
	case gspanner.TypeCode_INT64:
		s := v.GetStringValue()
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	case gspanner.TypeCode_FLOAT64:
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
		return base64.StdEncoding.DecodeString(v.GetStringValue())
	}
	return nil, fmt.Errorf("unsupport type: type:%v value:%T", t, v.Kind)
}

func (c *Column) marshalStruct(t *gspanner.StructType, fs *structpb.ListValue) (map[string]interface{}, error) {
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

func (c *Column) marshalList(t *gspanner.Type, l *structpb.ListValue) ([]interface{}, error) {
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
