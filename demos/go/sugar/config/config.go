/*	Package config populates your config structs from multiple sources

	Create a config instance with a call to New().

	Define configuration flags with calls to `AddFlags()`, if an error occurs
	adding flags (for example redefining a flag), following calls to AddFlags()
	will do nothing. Calls to `Err()` will return the first error from calls to
	`AddFlags()`.

	Flags have a name (that is automatically lowered on definition) and optionally
	a category, that will be used to find the flag in INI files, environment
	variables and command-line flags.

	Configure the way the config is parsed with calls to `SetAppName()`,
	`SetEnvPrefix()`, `SetFilePaths()`, `SetFileName()`.
	By default the application name defaults to the executable name, the
	environment prefix is empty, the default configuration file to look for is
	`config.ini` and the only file paths to find for this file is `.`.

	Call `Parse()` method to load the configuration from the sources. If a
	previous `AddFlags()` call failed, `Parse()` will just return that error.
	It can return an error too if the config file is not found or if one of the
	flags was not found in the sources and a default value for it was not given.

	If a call to `Parse()` succeeds, following calls to `Parsed()` will return
	true and calls to `Err()` will return nil.

	Once the configuration is defined a call to `Parse()` will look for configuration
	in the sources (by priority):

		INI config file:

			Flags in INI files are found with the category name (matching the INI section),
			and the flag name (matching the INI variable name).

			By default, `config.ini` in the working directory file path `.` will be
			looked up. This can be changed with calls to `Config.SetFileName()` and
			`Config.SetFilePaths()`.

			By default, if no config file is found, no error will be thrown (if all
			required flags have been defined). The exception to this is when a --config
			option is given as a command line argument. In this case, if the file
			is not found in any of the file paths, an error will be returned.

		Environment variables:

			Flags can be defined by environment variables (in uppercase) of the form:

			`{ENV_PREFIX}_{CATEGORY}_{NAME}` or `{ENV_PREFIX}_{NAME}`.

			The environment prefix is `NX` by default. This can be changed with a
			call to `Config.SetEnvPrefix()`.

		Command-line flags:

			Flags in command line are looked up in one of the following forms:

				`--{category}-{name} {value}`

				`--{category}-{name}={value}`

			Boolean flags without a value are considered to be true.

			Short forms for the flags (one character) can be defined. The category
			is not used when parsing short flags. Short bool flags can be combined:

				`-a=hello -b -c bye -d` is equivalent to `--category-long-name-of-a=hello -c="bye" -bd`

		Default values as defined in-code:

			A default value can be defined for each flag. If a flag has no value
			in the configuration and no default value is given for it,
			`Config.Parse()` will fail.


	`Config` can be `Parse()`d explicitly or you can check the error of
	`NewServiceFromConfig()` that parses the config if it hasn't been parsed
	and returns any error from config parsing.
*/

package config

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jaracil/nxcli/demos/go/sugar/service"
	flag "github.com/ogier/pflag"
)

var Config *Cfgo

type Cfgo struct {
	Name        string
	EnvPrefix   string
	FileName    string
	FilePaths   []string
	flags       map[string]map[string]*Flag
	flagOrder   map[string][]string
	shorts      map[string]*Flag
	err         error
	init        bool
	parsed      bool
	iniRequired bool
}

var ServiceConfig struct {
	Server           string  `name:"server" short:"s" description:"Nexus [tcp|ssl|ws|wss]://host[:port]"`
	User             string  `name:"user" short:"u" description:"Nexus username"`
	Password         string  `name:"pass" short:"p" description:"Nexus password"`
	Prefix           string  `name:"prefix" description:"Nexus listen prefix"`
	Pulls            int     `name:"pulls" description:"Number of concurrent nexus task pulls" default:"1"`
	PullTimeout      float64 `name:"pulltimeout" description:"Timeout for a nexus task to be pulled" default:"3600"`
	MaxThreads       int     `name:"maxthreads" description:"Maximum number of threads running concurrently" default:"-1"`
	StatsPeriod      float64 `name:"statsperiod" description:"Period in seconds for the service stats to be printed if debug is enabled" default:"300"`
	GracefulExitTime float64 `name:"gracefulexit" description:"Timeout for a graceful exit" default:"20"`
	Debug            bool    `name:"debug" description:"Debug output enabled" default:"false"`
}

func init() {
	Config = New()
	Config.SetEnvPrefix("NX")
	Config.SetFilePaths(".")
	Config.SetFileName("config.ini")
	Config.AddFlags("", &ServiceConfig)
}

// NewService creates a new nexus service from config
// If the config hasn't been parsed, it will be automatically parsed and return
// any error
func NewService() (*service.Service, error) {
	if !Config.Parsed() {
		if err := Config.Parse(); err != nil {
			return nil, err
		}
	}
	if ServiceConfig.MaxThreads == -1 {
		ServiceConfig.MaxThreads = runtime.NumCPU()
	}
	return &service.Service{
		Server:           ServiceConfig.Server,
		User:             ServiceConfig.User,
		Password:         ServiceConfig.Password,
		Prefix:           ServiceConfig.Prefix,
		Pulls:            ServiceConfig.Pulls,
		PullTimeout:      time.Second * time.Duration(ServiceConfig.PullTimeout),
		MaxThreads:       ServiceConfig.MaxThreads,
		DebugEnabled:     ServiceConfig.Debug,
		StatsPeriod:      time.Second * time.Duration(ServiceConfig.StatsPeriod),
		GracefulExitTime: time.Second * time.Duration(ServiceConfig.GracefulExitTime),
	}, nil
}

func New() *Cfgo {
	c := &Cfgo{
		Name:      os.Args[0],
		EnvPrefix: "",
		FileName:  "config.ini",
		FilePaths: []string{"."},
		flags: map[string]map[string]*Flag{
			"": {},
		},
		flagOrder: map[string][]string{
			"": {},
		},
		shorts:      map[string]*Flag{},
		err:         nil,
		init:        false,
		parsed:      false,
		iniRequired: false,
	}
	flag.Usage = c.printUsage
	return c
	//flag.Usage = func() {} // Help output, tweak here
}

func (c *Cfgo) SetEnvPrefix(prefix string) {
	c.EnvPrefix = strings.ToUpper(prefix) + "_"
}

func (c *Cfgo) SetFileName(name string) {
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

func (c *Cfgo) AddFlags(category string, config interface{}) {
	if c.err != nil {
		return
	}

	// Save the error on panic
	errs := fmt.Sprintf(`config: adding flags %+v`, config)
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
		return
	}

	// Check pointer
	pval := reflect.ValueOf(config)
	if pval.Type().Kind() != reflect.Ptr {
		c.err = fmt.Errorf("%s: expecting pointer to a struct, got %s", errs, pval.Type().Kind().String())
		return
	}

	// Check struct
	sval := pval.Elem()
	if sval.Type().Kind() != reflect.Struct {
		c.err = fmt.Errorf("%s: expecting pointer to a struct, got %s", errs, sval.Type().Kind().String())
		return
	}

	// Add category if missing
	category = strings.ToLower(category)
	cat, ok := c.flags[category]
	if !ok {
		c.flags[category] = map[string]*Flag{}
		c.flagOrder[category] = []string{}
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
					return
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
				return
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
						c.err = fmt.Errorf("%s: invalid default value '%s' on (%s) field: %s", errs, defStr, name, perr.Error())
						return
					}
					flag.Default = &DefaultValue{defStr}
				case reflect.Float64:
					_, perr := strconv.ParseFloat(defStr, 64)
					if perr != nil {
						c.err = fmt.Errorf("%s: invalid default value '%s' on (%s) field: %s", errs, defStr, name, perr.Error())
						return
					}
					flag.Default = &DefaultValue{defStr}
				case reflect.Bool:
					_, perr := strconv.ParseBool(defStr)
					if perr != nil {
						c.err = fmt.Errorf("%s: invalid default value '%s' on (%s) field: %s", errs, defStr, name, perr.Error())
						return
					}
					flag.Default = &DefaultValue{defStr}
				default:
					c.err = fmt.Errorf("%s: unsupported type '%s' on (%s) field: only [string, bool, int, int64, float64] types supported", errs, kind.String(), name)
					return
				}
			}

			// Add flag to category map
			cat[flag.Name] = flag

			// Add flag to category order list
			c.flagOrder[category] = append(c.flagOrder[category], flag.Name)

			// Add flag to shorts map
			if short != "" {
				c.shorts[short] = flag
			}
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
}

func (c *Cfgo) Parse() error {
	if c.err != nil {
		return c.err
	}

	// Look for config file
	c.setConfigPathFromCmd()

	// Init flags
	if !c.init {
		flag.String("config", "config.ini", "Use the configuration file provided")
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
					return fmt.Errorf("config: flag (%s): no value found and no default value provided", fl.CmdName())
				} else {
					if err := flag.Set(fl.CmdName(), fl.Default.Value); err != nil {
						return fmt.Errorf("config: flag (%s): applying default value '%s': %s", fl.CmdName(), fl.Default.Value, err.Error())
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
	if c.iniRequired || path.IsAbs(c.FileName) {
		f, err := os.Open(c.FileName)
		if err != nil {
			return fmt.Errorf("config: loading config file (%s): %s", c.FileName, err.Error())
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
	if !found && c.iniRequired {
		tried := []string{}
		for _, fp := range c.FilePaths {
			tried = append(tried, fp+"/"+c.FileName)
		}
		return fmt.Errorf("config: no config file found: tried %v", tried)
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
			c.iniRequired = true
			return
		}
		if strings.HasPrefix(arg, "-config=") || strings.HasPrefix(arg, "--config=") {
			if m := rxp.FindStringSubmatch(arg); m != nil {
				c.FileName = m[1]
				c.iniRequired = true
				return
			}
		}
	}
	c.iniRequired = false
}

func (c *Cfgo) printUsage() {
	fmt.Fprintf(os.Stderr, "usage: %s [<flags>]\n\n", c.Name)
	cf := &Flag{
		Name:        "config",
		Category:    "",
		Description: "Use the configuration file provided",
	}
	fmt.Fprintf(os.Stderr, "config sources:\n")
	cf.PrintUsage()
	fmt.Fprintf(os.Stderr, "\nflags:\n")
	for _, fn := range c.flagOrder[""] {
		c.flags[""][fn].PrintUsage()
	}
	for cat, flags := range c.flags {
		if cat != "" {
			fmt.Fprintf(os.Stderr, "\n%s flags:\n", cat)
			for _, fn := range c.flagOrder[cat] {
				flags[fn].PrintUsage()
			}
		}
	}
}
