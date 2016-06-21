package config

import (
	"flag"
	"fmt"
	"reflect"
	"strings"
)

type Flag struct {
	cfgo        *Cfgo
	Name        string
	Short       string
	Category    string
	Description string
	Default     *DefaultValue
	Value       reflect.Value
	hasBeenSet  bool
}

type DefaultValue struct {
	Value string
}

func (f *Flag) EnvName() string {
	if f.Category == "" {
		return fmt.Sprintf("%s%s", f.cfgo.EnvPrefix, strings.ToUpper(f.Name))
	}
	return fmt.Sprintf("%s%s_%s", f.cfgo.EnvPrefix, strings.ToUpper(f.Category), strings.ToUpper(f.Name))
}

func (f *Flag) CmdName() string {
	if f.Category == "" {
		return strings.ToLower(f.Name)
	}
	return fmt.Sprintf("%s-%s", strings.ToLower(f.Category), strings.ToLower(f.Name))
}

func (f *Flag) init() error {
	name := f.CmdName()
	//if f.Value.Type() == reflect.TypeOf(time.Duration(0)) {
	//	v := f.Value.Addr().Interface().(*time.Duration)
	//	flag.DurationVar(v, name, *v, f.Description)
	//	return
	//}
	switch f.Value.Kind() {
	case reflect.String:
		v := f.Value.Addr().Interface().(*string)
		flag.StringVar(v, name, *v, f.Description)
		if f.Short != "" {
			flag.StringVar(v, f.Short, *v, "")
		}
	case reflect.Int:
		v := f.Value.Addr().Interface().(*int)
		flag.IntVar(v, name, *v, f.Description)
		if f.Short != "" {
			flag.IntVar(v, f.Short, *v, "")
		}
	case reflect.Int64:
		v := f.Value.Addr().Interface().(*int64)
		flag.Int64Var(v, name, *v, f.Description)
		if f.Short != "" {
			flag.Int64Var(v, f.Short, *v, "")
		}
	case reflect.Float64:
		v := f.Value.Addr().Interface().(*float64)
		flag.Float64Var(v, name, *v, f.Description)
		if f.Short != "" {
			flag.Float64Var(v, f.Short, *v, "")
		}
	case reflect.Bool:
		v := f.Value.Addr().Interface().(*bool)
		flag.BoolVar(v, name, *v, f.Description)
		if f.Short != "" {
			flag.BoolVar(v, f.Short, *v, "")
		}
	//case reflect.Slice:
	//	switch f.Value.Type() {
	//	case intSliceType:
	//		v := f.Value.Addr().Interface().(*[]int)
	//		v1 := NewIntSlice(v)
	//		flag.Var(v1, name, f.Description)
	//	case strSliceType:
	//		v := f.Value.Addr().Interface().(*[]string)
	//		v1 := NewStrSlice(v)
	//		flag.Var(v1, name, f.Description)
	//	case floatSliceType:
	//		v := f.Value.Addr().Interface().(*[]float64)
	//		v1 := NewFloatSlice(v)
	//		flag.Var(v1, name, f.Description)
	//	default:
	//		log.Fatalf("unexpected type for flag (%v)", f)
	//	}
	default:
		return fmt.Errorf("unexpected type for flag (%v): %s", f, f.Value.Kind().String())
	}
	return nil
}
