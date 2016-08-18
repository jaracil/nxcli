// Package sugar is boilerplate code to make writing services more sweet.

package sugar

import (
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/jaracil/ei"
	. "github.com/jaracil/nxcli/demos/go/sugar/log"
	"github.com/jaracil/nxcli/demos/go/sugar/service"
	nexus "github.com/jaracil/nxcli/nxcore"
)

type ServiceOpts struct {
	Path        string
	Pulls       int
	PullTimeout time.Duration
	MaxThreads  int
	Testing     bool
}

// NewService creates a new nexus service
// If passed ServiceOpts is nil the defaults are 1 pull, an hour of pullTimeout and runtime.NumCPU() maxThreads
// Debug output is disabled by deafult
// StatsPeriod defaults to 5 minutes
// GracefulExitTime defaults to 20 seconds
func NewService(url string, opts *ServiceOpts) *service.Service {
	url, username, password := parseServerUrl(url)
	opts = populateOpts(opts)
	return &service.Service{Name: "", Url: url, User: username, Pass: password, Path: opts.Path, Pulls: opts.Pulls, PullTimeout: opts.PullTimeout, MaxThreads: opts.MaxThreads, LogLevel: "info", StatsPeriod: time.Minute * 5, GracefulExit: time.Second * 20, Testing: opts.Testing}
}

// ReplyToWrapper is a wrapper for methods
// If a replyTo map parameter is set with a type parameter (with "pipe" or "service" values) and a path
// parameter with the service path or pipeId to respond to, the usual SendError/SendResult pattern will
// be skipped and the answer will go to the pipe or service specified after doing an Accept() to the task.
func ReplyToWrapper(f func(*nexus.Task) (interface{}, *nexus.JsonRpcErr)) func(*nexus.Task) (interface{}, *nexus.JsonRpcErr) {
	return func(t *nexus.Task) (interface{}, *nexus.JsonRpcErr) {
		var repTy, repPath string
		var ok bool
		if replyTo, err := ei.N(t.Params).M("replyTo").MapStr(); err != nil {
			return f(t)
		} else {
			if repPath, ok = replyTo["path"].(string); !ok {
				return f(t)
			}
			if repTy, ok = replyTo["type"].(string); !ok || (repTy != "pipe" && repTy != "service") {
				return f(t)
			}
		}
		res, errm := f(t)
		t.Tags["@local@repliedTo"] = true
		_, err := t.Accept()
		if err != nil {
			Log(WarnLevel, "replyto wrapper", "could not accept task: %s", err.Error())
		} else if repTy == "pipe" {
			if pipe, err := t.GetConn().PipeOpen(repPath); err != nil {
				Log(WarnLevel, "replyto wrapper", "could not open received pipeId (%s): %s", repPath, err.Error())
			} else if _, err = pipe.Write(map[string]interface{}{"result": res, "error": errm}); err != nil {
				Log(WarnLevel, "replyto wrapper", "error writing response to pipe: %s", err.Error())
			}
		} else if repTy == "service" {
			if _, err := t.GetConn().TaskPush(repPath, map[string]interface{}{"result": res, "error": errm}, time.Second*30, &nexus.TaskOpts{Detach: true}); err != nil {
				Log(WarnLevel, "replyto wrapper", "could not push response task to received path (%s): %s", repPath, err.Error())
			}
		}
		return res, errm
	}
}

// IsNexusErr returns whether the err is a *nexus.JsonRpcErr
func IsNexusErr(err error) bool {
	_, ok := err.(*nexus.JsonRpcErr)
	return ok
}

// IsNexusErrCode returns whether the err is a *nexus.JsonRpcErr and matches the *nexus.JsonRpcErr.Cod
func IsNexusErrCode(err error, code int) bool {
	if nexusErr, ok := err.(*nexus.JsonRpcErr); ok {
		return nexusErr.Cod == code
	}
	return false
}

// Get url, user and pass
func parseServerUrl(server string) (string, string, string) {
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
	return server, username, password
}

// Set defaults for Opts
func populateOpts(opts *ServiceOpts) *ServiceOpts {
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
	return opts
}
