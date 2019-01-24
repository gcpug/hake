package sgcvj

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/spanner"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

type Column spanner.GenericColumnValue

var _ json.Marshaler = (*Column)(nil)

func (c *Column) MarshalJSON() ([]byte, error) {
	v, err := c.marshal(c.Value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func (c *Column) marshal(v *structpb.Value) (interface{}, error) {
	switch v.Kind.(type) {
	case *structpb.Value_NullValue:
		return nil, nil
	case *structpb.Value_NumberValue:
		return v.GetNumberValue(), nil
	case *structpb.Value_StringValue:
		return v.GetStringValue(), nil
	case *structpb.Value_BoolValue:
		return v.GetBoolValue(), nil
	case *structpb.Value_StructValue:
		return c.marshalStruct(v.GetStructValue())
	case *structpb.Value_ListValue:
		return c.marshalList(v.GetListValue())
	}
	return nil, fmt.Errorf("unsupport type: %T", v.Kind)
}

func (c *Column) marshalStruct(s *structpb.Struct) (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(s.Fields))

	for n := range s.Fields {
		v, err := c.marshal(s.Fields[n])
		if err != nil {
			return nil, err
		}
		m[n] = v
	}

	return m, nil
}

func (c *Column) marshalList(l *structpb.ListValue) ([]interface{}, error) {
	vs := make([]interface{}, len(l.Values))

	for i := range l.Values {
		v, err := c.marshal(l.Values[i])
		if err != nil {
			return nil, err
		}
		vs[i] = v
	}

	return vs, nil
}
