package sugar

import (
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	nxcli "github.com/jaracil/nxcli"
	. "github.com/jaracil/nxcli/demos/go/sugar/log"
	"github.com/jaracil/nxcli/demos/go/sugar/service"
)

type Server struct {
	Server           string
	User             string
	Password         string
	Pulls            int
	PullTimeout      time.Duration
	MaxThreads       int
	StatsPeriod      time.Duration
	GracefulExitTime time.Duration
	LogLevel         string
	services         map[string]*service.Service
	wg               *sync.WaitGroup
}

func NewServer(server string, opts *ServiceOpts) *Server {
	server, username, password := parseServerUrl(server)
	opts = populateOpts(opts)
	return &Server{Server: server, User: username, Password: password, Pulls: opts.Pulls, PullTimeout: opts.PullTimeout, MaxThreads: opts.MaxThreads, LogLevel: "info", StatsPeriod: time.Minute * 5, GracefulExitTime: time.Second * 20, services: map[string]*service.Service{}}
}

func (s *Server) SetUrl(url string) {
	s.Server = url
}

func (s *Server) SetUser(user string) {
	s.User = user
}

func (s *Server) SetPass(password string) {
	s.Password = password
}

func (s *Server) SetLogLevel(l string) {
	s.LogLevel = l
}

func (s *Server) SetStatsPeriod(t time.Duration) {
	s.StatsPeriod = t
	if s.services != nil {
		for _, svc := range s.services {
			svc.SetStatsPeriod(t)
		}
	}
}

func (s *Server) SetGracefulExitTime(t time.Duration) {
	s.GracefulExitTime = t
	if s.services != nil {
		for _, svc := range s.services {
			svc.SetGracefulExitTime(t)
		}
	}
}

func (s *Server) AddService(name string, prefix string, opts *ServiceOpts) *service.Service {
	if s.services == nil {
		s.services = map[string]*service.Service{}
	}
	svc := &service.Service{Name: name, Server: s.Server, User: s.User, Password: s.Password, Prefix: prefix, Pulls: s.Pulls, PullTimeout: s.PullTimeout, MaxThreads: s.MaxThreads, LogLevel: s.LogLevel, StatsPeriod: s.StatsPeriod, GracefulExitTime: s.GracefulExitTime}
	if opts != nil {
		opts = populateOpts(opts)
		svc.Pulls = opts.Pulls
		svc.PullTimeout = opts.PullTimeout
		svc.MaxThreads = opts.MaxThreads
	}
	s.services[name] = svc
	return svc
}

func (s *Server) Serve() error {
	// Parse url
	_, err := url.Parse(s.Server)
	if err != nil {
		return fmt.Errorf("server: invalid nexus url (%s): %s", s.Server, err.Error())
	}

	// Dial
	nc, err := nxcli.Dial(s.Server, nxcli.NewDialOptions())
	if err != nil {
		return fmt.Errorf("server: can't connect to nexus server (%s): %s", s.Server, err.Error())
	}

	// Login
	_, err = nc.Login(s.User, s.Password)
	if err != nil {
		return fmt.Errorf("server: can't login to nexus server (%s) as (%s): %s", s.Server, s.User, err.Error())
	}

	// Configure services
	if s.services == nil || len(s.services) == 0 {
		return fmt.Errorf("server: no services to serve")
	}
	for _, svc := range s.services {
		svc.SetLogLevel(s.LogLevel)
		svc.SetConn(nc)
	}

	// Wait for signal
	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt)
		<-signalChan
		Log.Debugf("signal: received Ctrl+C: stop gracefuly")
		for _, svc := range s.services {
			svc.GracefulStop()
		}
		<-signalChan
		Log.Debugf("signal: received Ctrl+C again: stop")
		for _, svc := range s.services {
			svc.Stop()
		}
	}()

	// Serve
	s.wg = &sync.WaitGroup{}
	errCh := make(chan error, 0)
	for _, svc := range s.services {
		s.wg.Add(1)
		go func(serv *service.Service) {
			if err := serv.Serve(); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
			s.wg.Done()
		}(svc)
	}

	var serveErr error
	s.wg.Wait()
	select {
	case serveErr = <-errCh:
	default:
	}
	return serveErr

}
