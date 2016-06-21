package config

import (
	"flag"
	"fmt"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var Config *Cfgo
var _defConfigFile = "config.ini"
var _specifiedConfigFile = false

type Cfgo struct {
	Name      string
	EnvPrefix string
	FileName  string
	FilePaths []string
	flags     map[string]map[string]*Flag
	shorts    map[string]*Flag
	err       error
	init      bool
	parsed    bool
}

func init() {
	// Init singleton
	Config = &Cfgo{
		Name:      os.Args[0],
		EnvPrefix: "NX_",
		FileName:  _defConfigFile,
		FilePaths: []string{"."},
		flags: map[string]map[string]*Flag{
			"": {},
		},
		shorts: map[string]*Flag{},
		err:    nil,
		init:   false,
		parsed: false,
	}
	//flag.Usage = func() {} // Help output, tweak here
}

func (c *Cfgo) SetEnvPrefix(prefix string) {
	c.EnvPrefix = strings.ToUpper(prefix) + "_"
}

func (c *Cfgo) SetFilename(name string) {
	c.FileName = name
}

func (c *Cfgo) SetAppName(name string) {
	c.Name = name
}

func (c *Cfgo) SetFilePaths(paths ...string) {
	c.FilePaths = []string{}
	for _, p := range paths {
		c.FilePaths = append(c.FilePaths, path.Clean(p))
	}
}

func (c *Cfgo) Parsed() bool {
	return c.parsed
}

func (c *Cfgo) Err() error {
	return c.err
}

func (c *Cfgo) AddConfig(category string, config interface{}) *Cfgo {
	if c.err != nil {
		return c
	}

	// Save the error on panic
	errs := fmt.Sprintf(`adding flags %+v`, config)
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			nerr, ok := r.(error)
			if !ok {
				c.err = fmt.Errorf("%s: pkg: %v", errs, r)
			} else {
				c.err = fmt.Errorf("%s: %s", errs, nerr.Error())
			}
		}
	}()

	if c.init {
		c.err = fmt.Errorf("%s: cannot AddFlags() after a call to Parse()", errs)
		return c
	}

	// Check pointer
	pval := reflect.ValueOf(config)
	if pval.Type().Kind() != reflect.Ptr {
		c.err = fmt.Errorf("%s: expecting pointer to a struct, got %s", errs, pval.Type().Kind().String())
		return c
	}

	// Check struct
	sval := pval.Elem()
	if sval.Type().Kind() != reflect.Struct {
		c.err = fmt.Errorf("%s: expecting pointer to a struct, got %s", errs, sval.Type().Kind().String())
		return c
	}

	// Add category if missing
	category = strings.ToLower(category)
	cat, ok := c.flags[category]
	if !ok {
		c.flags[category] = map[string]*Flag{}
		cat = c.flags[category]
	}

	// For each field of the struct
	for i := 0; i < sval.NumField(); i++ {
		f := sval.Type().Field(i)
		if f.PkgPath == "" { // only public fields
			sfield := sval.Type().Field(i)
			field := sval.Field(i)
			ty := sfield.Type
			kind := ty.Kind()
			name := sfield.Name
			tag := sfield.Tag

			defStr := strings.TrimSpace(tag.Get("default"))
			hasDef := strings.Contains(string(tag), `default:"`)
			nname := strings.ToLower(tag.Get("name"))
			if nname == "" {
				nname = strings.ToLower(name)
			}
			short := strings.TrimSpace(strings.ToLower(tag.Get("short")))
			if short != "" {
				if len(short) != 1 {
					c.err = fmt.Errorf("%s: flag (%s) short form must be only one character length: got '%s'", errs, nname, short)
					return c
				}
				if _, ok := c.shorts[short]; ok {
					c.err = fmt.Errorf("%s: flag (%s) short mode (%s) is already defined", errs, nname, short)
					if category != "" {
						c.err = fmt.Errorf("%s in category (%s)", c.err.Error(), category)
					}
				}
			}
			if _, ok := cat[nname]; ok {
				c.err = fmt.Errorf("%s: flag (%s) is already defined", errs, nname)
				if category != "" {
					c.err = fmt.Errorf("%s in category (%s)", c.err.Error(), category)
				}
				return c
			}
			flag := &Flag{
				cfgo:        c,
				Name:        nname,
				Short:       short,
				Category:    category,
				Description: tag.Get("description"),
				Default:     nil,
				Value:       field,
			}

			// Gather default value
			if hasDef {
				switch kind {
				case reflect.String:
					flag.Default = &DefaultValue{defStr}
				case reflect.Int, reflect.Int64:
					_, perr := strconv.Atoi(defStr)
					if perr != nil {
						c.err = fmt.Errorf("invalid default value '%s' on (%s) field: %s", defStr, name, perr.Error())
						return c
					}
					flag.Default = &DefaultValue{defStr}
				case reflect.Float64:
					_, perr := strconv.ParseFloat(defStr, 64)
					if perr != nil {
						c.err = fmt.Errorf("invalid default value '%s' on (%s) field: %s", defStr, name, perr.Error())
						return c
					}
					flag.Default = &DefaultValue{defStr}
				case reflect.Bool:
					_, perr := strconv.ParseBool(defStr)
					if perr != nil {
						c.err = fmt.Errorf("invalid default value '%s' on (%s) field: %s", defStr, name, perr.Error())
						return c
					}
					flag.Default = &DefaultValue{defStr}
				default:
					c.err = fmt.Errorf("unsupported type '%s' on (%s) field: only [string, bool, int, int64, float64] types supported", kind.String(), name)
					return c
				}
			}

			// Add flag to category map
			cat[flag.Name] = flag

			// Add flag to shorts map
			c.shorts[short] = flag
		}
	}

	// Debug
	//fmt.Printf("added category '%s' flags:\n", category)
	//for f, v := range cat {
	//	if v.Default != nil {
	//		fmt.Printf("\t%s = (Name:%s Default:%+v, Description:%s)\n", f, v.Name, v.Default.Value, v.Description)
	//	} else {
	//		fmt.Printf("\t%s = (Name:%s Default:nil, Description:%s)\n", f, v.Name, v.Description)
	//	}
	//}
	return c
}

func (c *Cfgo) Parse() error {
	if c.err != nil {
		return c.err
	}

	// Look for config file
	c.setConfigPathFromCmd()

	// Init flags
	if !c.init {
		flag.String("config", "", "Path to configuration file")
		for _, c := range c.flags {
			for _, f := range c {
				f.init()
			}
		}
		c.init = true
	}
	for _, c := range c.flags {
		for _, f := range c {
			f.hasBeenSet = false
		}
	}

	// Get config from ini file, error if specified a config file (name or path) and it's not found
	if err := c.setFromIni(); err != nil {
		c.err = err
		return c.err
	}

	// Get config from environment variables
	c.setFromEnv()

	// Get config from command line parameters
	c.setFromCmd()

	// Get config from defaults
	if err := c.setFromDefaults(); err != nil {
		c.err = err
		return c.err
	}

	c.parsed = true
	c.err = nil

	return nil
}

func (c *Cfgo) setFromDefaults() error {
	flag.Visit(func(f *flag.Flag) {
		if len(f.Name) == 1 {
			if fl, ok := c.shorts[f.Name]; ok && fl.Short == f.Name {
				fl.hasBeenSet = true
			}
		}
		spl := strings.SplitN(f.Name, "-", 2)
		if len(spl) == 2 {
			if category, ok := c.flags[spl[0]]; ok {
				if fl, ok := category[spl[1]]; ok {
					fl.hasBeenSet = true
				}
			}
		} else {
			if fl, ok := c.flags[""][f.Name]; ok {
				fl.hasBeenSet = true
			}
		}
	})
	for _, cat := range c.flags {
		for _, fl := range cat {
			if !fl.hasBeenSet {
				if fl.Default == nil {
					return fmt.Errorf("flag (%s): no value found and no default value provided", fl.CmdName())
				} else {
					if err := flag.Set(fl.CmdName(), fl.Default.Value); err != nil {
						return fmt.Errorf("flag (%s): applying default value '%s': %s", fl.CmdName(), fl.Default.Value, err.Error())
					}
				}
			}
		}
	}
	return nil
}

func (c *Cfgo) setFromCmd() {
	flag.Parse()
}

func (c *Cfgo) setFromEnv() {
	for _, cat := range c.flags {
		for _, f := range cat {
			if val := os.Getenv(f.EnvName()); val != "" {
				flag.Set(f.CmdName(), val)
				f.hasBeenSet = true
			}
		}
	}
}

func (c *Cfgo) setFromIni() error {
	// Absolute path
	if _specifiedConfigFile || path.IsAbs(c.FileName) {
		f, err := os.Open(c.FileName)
		if err != nil {
			return fmt.Errorf("loading config file (%s): %s", c.FileName, err.Error())
		}
		defer f.Close()
		c.setFromMap(parseIniFile(f))
		return nil
	}

	// Relative path (found)
	if len(c.FilePaths) == 0 {
		c.FilePaths = []string{"."}
	}
	found := false
	for _, fp := range c.FilePaths {
		f, err := os.Open(fp + "/" + c.FileName)
		if err == nil {
			defer f.Close()
			c.setFromMap(parseIniFile(f))
			found = true
			break
		}
	}

	// Relative path (not found), fail if it was specified
	if !found && _specifiedConfigFile {
		tried := []string{}
		for _, fp := range c.FilePaths {
			tried = append(tried, fp+"/"+c.FileName)
		}
		return fmt.Errorf("no config file found: tried %v", tried)
	}

	return nil
}

func (c *Cfgo) setFromMap(m map[string]map[string]string) {
	for cn, cat := range c.flags {
		if catm, ok := m[cn]; ok {
			for _, f := range cat {
				if val, ok := catm[f.CmdName()]; ok {
					flag.Set(f.CmdName(), val)
					f.hasBeenSet = true
				}
			}
		}
	}
}

func (c *Cfgo) setConfigPathFromCmd() {
	rxp := regexp.MustCompile(`^[^=]+="?(.+?)"?$`)
	for i, arg := range os.Args[1:] {
		if (arg == "-config" || arg == "--config") && i < len(os.Args)-2 {
			c.FileName = os.Args[i+2]
			_specifiedConfigFile = true
			return
		}
		if strings.HasPrefix(arg, "-config=") || strings.HasPrefix(arg, "--config=") {
			if m := rxp.FindStringSubmatch(arg); m != nil {
				c.FileName = m[1]
				_specifiedConfigFile = true
				return
			}
		}
	}
	_specifiedConfigFile = false
}
