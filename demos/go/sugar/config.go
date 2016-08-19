package sugar

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jaracil/ei"
	. "github.com/jaracil/nxcli/demos/go/sugar/log"
	flag "github.com/ogier/pflag"
)

type ServerConfig struct {
	Url          string
	User         string
	Pass         string
	LogLevel     string
	GracefulExit float64
	Testing      bool
	Pulls        int
	PullTimeout  float64
	MaxThreads   int
	StatsPeriod  float64
	Services     map[string]ServiceConfig
}

type ServiceConfig struct {
	Path        string
	Pulls       int
	PullTimeout float64
	MaxThreads  int
}

type ServerFromConfig struct {
	Server
}

var config map[string]interface{}
var configFile string
var configParsed = false
var configServer ServerConfig

var MissingConfigErr = "missing parameter (%s) on config file"
var InvalidConfigErr = "invalid parameter (%s) on config file: %s"
var _logLevels = []string{"debug", "info", "warn", "error", "fatal", "panic"}

func parseConfig() error {
	if !configParsed {
		configServer = ServerConfig{
			LogLevel:     "info",
			GracefulExit: 20,
			Testing:      false,
			Pulls:        1,
			PullTimeout:  3600,
			MaxThreads:   4,
			StatsPeriod:  1800,
			Services:     map[string]ServiceConfig{},
		}

		// Get config file name
		flag.StringVarP(&configFile, "config", "c", "config.json", "JSON configuration file")
		flag.Parse()

		// Open file
		cf, err := os.Open(configFile)
		defer cf.Close()
		if err != nil {
			return fmt.Errorf("can't open config file (%s): %s", configFile, err.Error())
		}

		// Parse file
		dec := json.NewDecoder(cf)
		config = map[string]interface{}{}
		err = dec.Decode(&config)
		if err != nil {
			return fmt.Errorf("can't parse config file (%s): %s", configFile, err.Error())
		}

		// Get server config
		if _, ok := config["server"]; !ok {
			return fmt.Errorf(MissingConfigErr, "server")
		}
		server, err := ei.N(config).M("server").MapStr()
		if err != nil {
			return fmt.Errorf(InvalidConfigErr, "server", "must be map")
		}
		if _, ok := server["url"]; !ok {
			return fmt.Errorf(MissingConfigErr, "server.url")
		}
		if configServer.Url, err = ei.N(server).M("url").String(); err != nil {
			return fmt.Errorf(InvalidConfigErr, "server.user", "must be string")
		}
		if _, ok := server["user"]; !ok {
			return fmt.Errorf(MissingConfigErr, "server.user")
		}
		if configServer.User, err = ei.N(server).M("user").String(); err != nil {
			return fmt.Errorf(InvalidConfigErr, "server.user", "must be string")
		}
		if _, ok := server["pass"]; !ok {
			return fmt.Errorf(MissingConfigErr, "server.pass")
		}
		if configServer.Pass, err = ei.N(server).M("pass").String(); err != nil {
			return fmt.Errorf(InvalidConfigErr, "server.pass", "must be string")
		}
		if lli, ok := server["log-level"]; ok {
			if ll, err := ei.N(lli).String(); err == nil {
				if !inStrSlice(ll, _logLevels) {
					return fmt.Errorf(InvalidConfigErr, "server.log-level", fmt.Sprintf("must be one of [%+v]", _logLevels))
				}
				configServer.LogLevel = ll
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.log-level", "must be string")
			}
		}
		if gei, ok := server["graceful-exit"]; ok {
			if ge, err := ei.N(gei).Float64(); err == nil {
				if ge < 0 {
					return fmt.Errorf(InvalidConfigErr, "server.graceful-exit", "must be positive")
				}
				configServer.GracefulExit = ge
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.graceful-exit", "must be float")
			}
		}
		if tsi, ok := server["testing"]; ok {
			if ts, err := ei.N(tsi).Bool(); err == nil {
				configServer.Testing = ts
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.testing", "must be bool")
			}
		}
		if psi, ok := server["pulls"]; ok {
			if ps, err := ei.N(psi).Int(); err == nil {
				if ps < 1 {
					return fmt.Errorf(InvalidConfigErr, "server.pull", "must be positive")
				}
				configServer.Pulls = ps
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.pull", "must be int")
			}
		}
		if pti, ok := server["pull-timeout"]; ok {
			if pt, err := ei.N(pti).Float64(); err == nil {
				if pt < 0 {
					return fmt.Errorf(InvalidConfigErr, "server.pull-timeout", "must be positive or 0")
				}
				configServer.PullTimeout = pt
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.pull-timeout", "must be float")
			}
		}
		if mti, ok := server["max-threads"]; ok {
			if mt, err := ei.N(mti).Int(); err == nil {
				if mt < 1 {
					return fmt.Errorf(InvalidConfigErr, "server.max-threads", "must be positive")
				}
				configServer.MaxThreads = mt
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.max-threads", "must be int")
			}
		}
		if spi, ok := server["stats-period"]; ok {
			if sp, err := ei.N(spi).Float64(); err == nil {
				if sp < 1 {
					return fmt.Errorf(InvalidConfigErr, "server.stats-period", "must be positive")
				}
				configServer.StatsPeriod = sp
			} else {
				return fmt.Errorf(InvalidConfigErr, "server.stats-period", "must be float")
			}
		}

		// Get services config
		if _, ok := config["services"]; !ok {
			return fmt.Errorf(MissingConfigErr, "services")
		}
		services, err := ei.N(config).M("services").MapStr()
		if err != nil {
			return fmt.Errorf(InvalidConfigErr, "services", "must be map")
		}

		for name, optsi := range services {
			sc := ServiceConfig{
				Pulls:       configServer.Pulls,
				PullTimeout: configServer.PullTimeout,
				MaxThreads:  configServer.MaxThreads,
			}
			opts, err := ei.N(optsi).MapStr()
			if err != nil {
				return fmt.Errorf(InvalidConfigErr, "services."+name, "must be map")
			}
			if pt, err := ei.N(opts).M("path").String(); err != nil {
				return fmt.Errorf(MissingConfigErr, "services."+name+".path")
			} else if pt == "" {
				return fmt.Errorf(InvalidConfigErr, "services."+name+".path", "must not be empty")
			} else {
				sc.Path = pt
			}
			if _, ok := opts["pulls"]; ok {
				if ps, err := ei.N(opts).M("pulls").Int(); err == nil {
					if ps < 1 {
						return fmt.Errorf(InvalidConfigErr, "services."+name+".pulls", "must be positive")
					}
					sc.Pulls = ps
				} else {
					return fmt.Errorf(InvalidConfigErr, "services."+name+".pulls", "must be int")
				}
			}
			if _, ok := opts["pull-timeout"]; ok {
				if pt, err := ei.N(opts).M("pull-timeout").Float64(); err == nil {
					if pt < 0 {
						return fmt.Errorf(InvalidConfigErr, "services."+name+".pull-timeout", "must be positive or 0")
					}
					sc.PullTimeout = pt
				} else {
					return fmt.Errorf(InvalidConfigErr, "services."+name+".pull-timeout", "must be float")
				}
			}
			if _, ok := opts["max-threads"]; ok {
				if mt, err := ei.N(opts).M("max-threads").Int(); err == nil {
					if mt < 1 {
						return fmt.Errorf(InvalidConfigErr, "services."+name+".max-threads", "must be positive")
					}
					sc.MaxThreads = mt
				} else {
					return fmt.Errorf(InvalidConfigErr, "services."+name+".max-threads", "must be int")
				}
			}
			configServer.Services[name] = sc
		}
		configParsed = true
	}
	return nil
}

// NewServerFromConfig creates a new nexus server from config to which services can be added
func NewServerFromConfig() (*ServerFromConfig, error) {
	if err := parseConfig(); err != nil {
		Log(ErrorLevel, "config", err.Error())
		return nil, err
	}
	return &ServerFromConfig{
		Server{
			Url:          configServer.Url,
			User:         configServer.User,
			Pass:         configServer.Pass,
			Pulls:        configServer.Pulls,
			PullTimeout:  time.Duration(configServer.PullTimeout * float64(time.Second)),
			MaxThreads:   configServer.MaxThreads,
			LogLevel:     configServer.LogLevel,
			StatsPeriod:  time.Duration(configServer.StatsPeriod * float64(time.Second)),
			GracefulExit: time.Duration(configServer.GracefulExit * float64(time.Second)),
			Testing:      configServer.Testing,
			services:     map[string]*Service{},
		},
	}, nil
}

// AddServiceFromConfig adds services to a server from config
func (s *ServerFromConfig) AddService(name string) (*Service, error) {
	if s.services == nil {
		s.services = map[string]*Service{}
	}
	svcfg, ok := configServer.Services[name]
	if !ok {
		err := fmt.Errorf(MissingConfigErr, "services."+name)
		Log(ErrorLevel, "config", err.Error())
		return nil, err
	}
	svc := &Service{Name: name, Url: s.Url, User: s.User, Pass: s.Pass, Path: svcfg.Path, Pulls: svcfg.Pulls, PullTimeout: time.Duration(svcfg.PullTimeout * float64(time.Second)), MaxThreads: svcfg.MaxThreads, LogLevel: s.LogLevel, StatsPeriod: s.StatsPeriod, GracefulExit: s.GracefulExit, Testing: s.Testing}
	s.services[name] = svc
	return svc, nil
}

// NewServiceFromConfig creates a new nexus service from config
func NewServiceFromConfig(name string) (*Service, error) {
	if err := parseConfig(); err != nil {
		Log(ErrorLevel, "config", err.Error())
		return nil, err
	}
	svc, ok := configServer.Services[name]
	if !ok {
		err := fmt.Errorf(MissingConfigErr, "services."+name)
		Log(ErrorLevel, "config", err.Error())
		return nil, err
	}
	return &Service{
		Name:         name,
		Url:          configServer.Url,
		User:         configServer.User,
		Pass:         configServer.Pass,
		Path:         svc.Path,
		Pulls:        svc.Pulls,
		PullTimeout:  time.Duration(float64(time.Second) * svc.PullTimeout),
		MaxThreads:   svc.MaxThreads,
		LogLevel:     configServer.LogLevel,
		StatsPeriod:  time.Duration(float64(time.Second) * configServer.StatsPeriod),
		GracefulExit: time.Duration(float64(time.Second) * configServer.GracefulExit),
		Testing:      configServer.Testing,
	}, nil
}

// GetConfig retrieves the parsed configuration
func GetConfig() (map[string]interface{}, error) {
	if err := parseConfig(); err != nil {
		Log(ErrorLevel, "config", err.Error())
		return nil, err
	}
	return config, nil
}
