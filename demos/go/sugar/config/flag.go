package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	flag "github.com/ogier/pflag"
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

func (f *Flag) PrintUsage() {
	s := ""
	if f.Short != "" {
		s = fmt.Sprintf("  -%s, --%s=", f.Short, f.Name)
	} else {
		s = fmt.Sprintf("  --%s=", f.Name)
	}
	ty := f.Value.Kind().String()
	if f.Default != nil {
		if ty == "string" {
			s += fmt.Sprintf("%q", f.Default.Value)
		} else {
			if ty == "float64" {
				ty = "float"
			}
			s += fmt.Sprintf("%s", f.Default.Value)
		}
	}
	if f.Value.IsValid() {
		s += fmt.Sprintf(" (%s)\n    \t%s\n", ty, f.Description)
	} else {
		s += fmt.Sprintf("\n    \t%s\n", f.Description)
	}
	fmt.Fprint(os.Stderr, s)
}

func (f *Flag) init() error {
	name := f.CmdName()
	switch f.Value.Kind() {
	case reflect.String:
		v := f.Value.Addr().Interface().(*string)
		if f.Short == "" {
			flag.StringVar(v, name, *v, f.Description)
		} else {
			flag.StringVarP(v, name, f.Short, *v, f.Description)
		}
	case reflect.Int:
		v := f.Value.Addr().Interface().(*int)
		if f.Short == "" {
			flag.IntVar(v, name, *v, f.Description)
		} else {
			flag.IntVarP(v, name, f.Short, *v, f.Description)
		}
	case reflect.Int64:
		v := f.Value.Addr().Interface().(*int64)
		if f.Short == "" {
			flag.Int64Var(v, name, *v, f.Description)
		} else {
			flag.Int64VarP(v, name, f.Short, *v, f.Description)
		}
	case reflect.Float64:
		v := f.Value.Addr().Interface().(*float64)
		if f.Short == "" {
			flag.Float64Var(v, name, *v, f.Description)
		} else {
			flag.Float64VarP(v, name, f.Short, *v, f.Description)
		}
	case reflect.Bool:
		v := f.Value.Addr().Interface().(*bool)
		if f.Short == "" {
			flag.BoolVar(v, name, *v, f.Description)
		} else {
			flag.BoolVarP(v, name, f.Short, *v, f.Description)
		}
	default:
		return fmt.Errorf("unexpected type for flag (%v): %s", f, f.Value.Kind().String())
	}
	return nil
}
