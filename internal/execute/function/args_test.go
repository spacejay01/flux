package function_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/internal/execute/function"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

func TestReadArgs(t *testing.T) {
	readArgs := func(spec interface{}, args map[string]values.Value) error {
		fargs := interpreter.NewArguments(values.NewObjectWithValues(args))
		return function.ReadArgs(spec, fargs, nil)
	}
	for _, tt := range []struct {
		name string
		args map[string]values.Value
		want interface{}
	}{
		{
			name: "Any",
			args: map[string]values.Value{
				"a": values.NewString("t0"),
				"b": values.NewInt(4),
			},
			want: &struct {
				A values.Value
				B values.Value
			}{
				A: values.NewString("t0"),
				B: values.NewInt(4),
			},
		},
		{
			name: "Strings",
			args: map[string]values.Value{
				"values": values.NewArrayWithBacking(
					semantic.NewArrayType(semantic.BasicString),
					[]values.Value{
						values.NewString("a"),
						values.NewString("b"),
						values.NewString("c"),
					},
				),
			},
			want: &struct {
				Values []string
			}{
				Values: []string{"a", "b", "c"},
			},
		},
		{
			name: "Object",
			args: map[string]values.Value{
				"columns": values.NewObjectWithValues(map[string]values.Value{
					"name":  values.NewString("foo"),
					"value": values.NewInt(4),
				}),
			},
			want: &struct {
				Columns map[string]values.Value
			}{
				Columns: map[string]values.Value{
					"name":  values.NewString("foo"),
					"value": values.NewInt(4),
				},
			},
		},
		{
			name: "TableObject",
			args: map[string]values.Value{
				"tables": &flux.TableObject{Kind: "from"},
				"column": values.NewString("_value"),
			},
			want: &struct {
				Tables *function.TableObject
				Column string
			}{
				Tables: &function.TableObject{
					TableObject: &flux.TableObject{
						Kind: "from",
					},
				},
				Column: "_value",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			typ := reflect.TypeOf(tt.want).Elem()
			got := reflect.New(typ).Interface()
			if err := readArgs(got, tt.args); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
