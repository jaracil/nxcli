// Package sugar is boilerplate code to make writing services more sweet.

package sugar

import (
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/jaracil/nxcli/demos/go/sugar/service"
	nexus "github.com/jaracil/nxcli/nxcore"
)

type ServiceOpts struct {
	Pulls       int
	PullTimeout time.Duration
	MaxThreads  int
}

// NewService creates a new nexus service
// If passed ServiceOpts is nil the defaults are 1 pull, an hour of pullTimeout and runtime.NumCPU() maxThreads
// Debug output is disabled by deafult
// StatsPeriod defaults to 5 minutes
// GracefulExitTime defaults to 20 seconds
func NewService(server string, prefix string, opts *ServiceOpts) *service.Service {
	var username string
	var password string
	if !strings.Contains(server, "://") {
		server = "tcp://" + server
	}
	parsed, err := url.Parse(server)
	if err == nil && parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
	}
	if opts == nil {
		opts = &ServiceOpts{
			Pulls:       1,
			PullTimeout: time.Hour,
			MaxThreads:  runtime.NumCPU(),
		}
	}
	if opts.Pulls <= 0 {
		opts.Pulls = 1
	}
	if opts.PullTimeout < 0 {
		opts.PullTimeout = 0
	}
	if opts.MaxThreads <= 0 {
		opts.MaxThreads = 1
	}
	return &service.Service{Server: server, User: username, Password: password, Prefix: prefix, Pulls: opts.Pulls, PullTimeout: opts.PullTimeout, MaxThreads: opts.MaxThreads, DebugEnabled: false, StatsPeriod: time.Minute * 5, GracefulExitTime: time.Second * 20}
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
