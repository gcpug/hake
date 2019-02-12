package hake

import "text/template"

var jsonSchemaTmpl = template.Must(
	template.New("jsonSchema").Delims("<<", ">>").Funcs(template.FuncMap{
		"isNotLast": func(i, n int) bool {
			return i+1 != n
		},
	}).Parse(`{
	<<$n := len .>>
	"$schema": "http://json-schema.org/draft-04/schema#",
	"type": "object",
	"required": [
		<<range $i, $e := . >>
		"<<.Name>>"<<if (isNotLast $i $n)>>,<<end>>
		<<end>>
	],
	"properties": {
		<<range $i, $e := . >>
			"<<.Name>>": <<.Schema>>
			<<if (isNotLast $i $n)>>,<<end>>
		<<end>>
	}
}`))
