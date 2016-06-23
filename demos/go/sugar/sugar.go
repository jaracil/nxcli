/*	Package sugar is boilerplate code to make writing services more sweet.

	`Config` allows to add flags (`AddFlags()`) to the ones defined by sugar itself.

	Flags have a name (that is automatically lowered on definition) and optionally
	a category, that will be used to find the flag in INI files, environment
	variables and command-line flags.

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

package sugar

import (
	"time"

	"runtime"

	"github.com/jaracil/nxcli/demos/go/sugar/config"
	"github.com/jaracil/nxcli/demos/go/sugar/service"
	nexus "github.com/jaracil/nxcli/nxcore"
)

// Config is the configuration parser for the service
var Config *config.Cfgo

// ServiceConfig is the configuration for a service that is being parsed
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
	Config = config.New()
	Config.SetEnvPrefix("NX")
	Config.SetFilePaths(".")
	Config.SetFileName("config.ini")
	Config.AddFlags("", &ServiceConfig)
}

// NewService creates a new nexus service from config
// If the config hasn't been parsed, it will be automatically parsed and return
// any error
func NewServiceFromConfig() (*service.Service, error) {
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

// NewServiceFromParams creates a new nexus service with defaults
// Debug is disabled
// StatsPeriod is set to 5 minutes
// GracefulExitTime is set to 20 seconds
func NewServiceFromParams(server string, user string, password string, prefix string, pulls int, pullTimeout time.Duration, maxthreads int) *service.Service {
	return &service.Service{
		Server:           server,
		User:             user,
		Password:         password,
		Prefix:           prefix,
		Pulls:            pulls,
		PullTimeout:      pullTimeout,
		MaxThreads:       maxthreads,
		DebugEnabled:     false,
		StatsPeriod:      time.Minute * 5,
		GracefulExitTime: time.Second * 20,
	}
}

// IsNexusErr eturns wheter the err is a *nexus.JsonRpcErr
func IsNexusErr(err error) bool {
	_, ok := err.(*nexus.JsonRpcErr)
	return ok
}

// IsNexusErrCode returns wheter the err is a *nexus.JsonRpcErr and matches the *nexus.JsonRpcErr.Cod
func IsNexusErrCode(err error, code int) bool {
	if nexusErr, ok := err.(*nexus.JsonRpcErr); ok {
		return nexusErr.Cod == code
	}
	return false
}
