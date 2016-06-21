// Package sugar is boilerplate code to make writing services more sweet.
package sugar

import (
	"time"

	"runtime"

	"github.com/jaracil/nxcli/demos/go/sugar/config"
	"github.com/jaracil/nxcli/demos/go/sugar/service"
	nexus "github.com/jaracil/nxcli/nxcore"
)

var Config struct {
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

// AddConfig adds configuration flags to be parsed
// Category allows to split flags in order to not collide
// The second parameter expects a pointer to a struct with exported fields
// Tags are supported for changing field *name*, adding a *description*,
// a *short* form for the flag or a *default* value
func AddConfig(category string, v interface{}) error {
	if err := config.Config.AddConfig(category, v).Err(); err != nil {
		return err
	}
	return nil
}

// ParseConfig parses the flags defined with `AddConfig()`
// in addition to the ones defined by sugar itsel.
// The priority order is:
//
// - If the INI config file is found, values from the config file
// - Environment variables
// - Command-line flags
// - Default values as defined in-code
// - After-parse overrides
//
func ParseConfig() error {
	if err := config.Config.AddConfig("", &Config).Err(); err != nil {
		return err

	}
	if err := config.Config.Parse(); err != nil {
		return err
	}
	return nil
}

// NewService creates a new nexus service from config
// If the config hasn't been parsed, it will be automatically parsed
func NewServiceFromConfig() (*service.Service, error) {
	if !config.Config.Parsed() {
		if err := ParseConfig(); err != nil {
			return nil, err
		}
	}
	if Config.MaxThreads == -1 {
		Config.MaxThreads = runtime.NumCPU()
	}
	return &service.Service{
		Server:           Config.Server,
		User:             Config.User,
		Password:         Config.Password,
		Prefix:           Config.Prefix,
		Pulls:            Config.Pulls,
		PullTimeout:      time.Second * time.Duration(Config.PullTimeout),
		MaxThreads:       Config.MaxThreads,
		DebugEnabled:     Config.Debug,
		StatsPeriod:      time.Second * time.Duration(Config.StatsPeriod),
		GracefulExitTime: time.Second * time.Duration(Config.GracefulExitTime),
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
