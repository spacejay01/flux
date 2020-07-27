package function

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/semantic"
)

// TransformationSpec defines a spec for creating a transformation.
type TransformationSpec interface {
	// CreateTransformation will construct a transformation
	// and dataset using the given dataset and administration object.
	CreateTransformation(id execute.DatasetID, a execute.Administration) (execute.Transformation, execute.Dataset, error)
}

type specFactory struct {
	t    reflect.Type
	kind plan.ProcedureKind
}

func (f *specFactory) createOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	ptr := reflect.New(f.t)
	elem := ptr.Elem()
	for i, n := 0, elem.NumField(); i < n; i++ {
		f := elem.Type().Field(i)
		arg := f.Tag.Get("flux")
		if arg == "-" {
			continue
		}

		isPipeArg := strings.HasPrefix(arg, "<-")
		if isPipeArg {
			arg = strings.TrimPrefix(arg, "<-")
		}

		if arg == "" {
			name := []rune(f.Name)
			if len(name) > 0 {
				name[0] = unicode.ToLower(name[0])
			}
			arg = string(name)
		}

		fv := elem.Field(i)
		if _, ok := fv.Interface().(*flux.TableObject); ok {
			parent, ok := args.Get(arg)
			if !ok {
				return nil, errors.Newf(codes.Invalid, "could not find %s parameter", arg)
			}
			p, ok := parent.(*flux.TableObject)
			if !ok {
				return nil, errors.Newf(codes.Invalid, "argument is not a table object: got %T", parent)
			}
			fv.Set(reflect.ValueOf(p))
			a.AddParent(p)
			continue
		}

		switch fv.Kind() {
		case reflect.String:
			v, ok, err := args.GetString(arg)
			if err != nil {
				return nil, err
			} else if !ok {
				continue
			}
			fv.SetString(v)
		default:
			panic("implement me")
		}
	}
	return &operationSpec{
		spec: &procedureSpec{
			kind: f.kind,
			spec: ptr.Interface().(TransformationSpec),
		},
	}, nil
}

type operationSpec struct {
	spec *procedureSpec
}

func (o *operationSpec) Kind() flux.OperationKind {
	return flux.OperationKind(o.spec.Kind())
}

type procedureSpec struct {
	plan.DefaultCost
	kind plan.ProcedureKind
	spec TransformationSpec
}

func newProcedureSpec(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*operationSpec)
	if !ok {
		return nil, errors.Newf(codes.Internal, "invalid spec type %T", qs)
	}
	return spec.spec, nil
}

func (p *procedureSpec) Kind() plan.ProcedureKind {
	return p.kind
}

func (p *procedureSpec) Copy() plan.ProcedureSpec {
	return p
}

func createTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*procedureSpec)
	if !ok {
		return nil, nil, errors.Newf(codes.Internal, "invalid spec type %T", spec)
	}
	return s.spec.CreateTransformation(id, a)
}

// RegisterTransformation will register a TransformationSpec
// in the pkgpath with name. The TransformationSpec is read from
// arguments using ReadArgs.
//
// The operation spec and procedure spec will be automatically generated
// for this transformation and the kind will be a concatenation of
// the pkgpath and name separated by a dot.
func RegisterTransformation(pkgpath, name string, spec TransformationSpec, signature semantic.MonoType) {
	kind := plan.ProcedureKind(fmt.Sprintf("%s.%s", pkgpath, name))
	factory := &specFactory{
		t:    reflect.TypeOf(spec).Elem(),
		kind: kind,
	}
	fn := flux.MustValue(flux.FunctionValue(name, factory.createOpSpec, signature))
	runtime.RegisterPackageValue(pkgpath, name, fn)
	plan.RegisterProcedureSpec(kind, newProcedureSpec, flux.OperationKind(kind))
	execute.RegisterTransformation(kind, createTransformation)
}
