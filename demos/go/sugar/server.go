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
)

type Server struct {
	Url          string
	User         string
	Pass         string
	Pulls        int
	PullTimeout  time.Duration
	MaxThreads   int
	StatsPeriod  time.Duration
	GracefulExit time.Duration
	LogLevel     string
	Testing      bool
	services     map[string]*Service
	wg           *sync.WaitGroup
}

func NewServer(url string) *Server {
	url, username, password := parseServerUrl(url)
	return &Server{Url: url, User: username, Pass: password, Pulls: 1, PullTimeout: time.Hour, MaxThreads: 4, LogLevel: "info", StatsPeriod: time.Minute * 5, GracefulExit: time.Second * 20, Testing: false, services: map[string]*Service{}}
}

func (s *Server) SetUrl(url string) {
	s.Url = url
}

func (s *Server) SetUser(user string) {
	s.User = user
}

func (s *Server) SetPass(password string) {
	s.Pass = password
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

func (s *Server) SetGracefulExit(t time.Duration) {
	s.GracefulExit = t
	if s.services != nil {
		for _, svc := range s.services {
			svc.SetGracefulExit(t)
		}
	}
}

func (s *Server) SetTesting(t bool) {
	s.Testing = t
	if s.services != nil {
		for _, svc := range s.services {
			svc.SetTesting(t)
		}
	}
}

func (s *Server) IsTesting() bool {
	return s.Testing
}

func (s *Server) AddService(name string, path string, opts *ServiceOpts) (*Service) {
	if s.services == nil {
		s.services = map[string]*Service{}
	}
	svc := &Service{Name: name, Url: s.Url, User: s.User, Pass: s.Pass, Path: path, Pulls: s.Pulls, PullTimeout: s.PullTimeout, MaxThreads: s.MaxThreads, LogLevel: s.LogLevel, StatsPeriod: s.StatsPeriod, GracefulExit: s.GracefulExit, Testing: s.Testing}
	if opts != nil {
		opts = populateOpts(opts)
		svc.Pulls = opts.Pulls
		svc.PullTimeout = opts.PullTimeout
		svc.MaxThreads = opts.MaxThreads
		svc.Testing = opts.Testing
	}
	s.services[name] = svc
	return svc
}

func (s *Server) Serve() error {
	// Parse url
	_, err := url.Parse(s.Url)
	if err != nil {
		err = fmt.Errorf("invalid nexus url (%s): %s", s.Url, err.Error())
		Log(ErrorLevel, "server", err.Error())
		return err
	}

	// Dial
	nc, err := nxcli.Dial(s.Url, nxcli.NewDialOptions())
	if err != nil {
		err = fmt.Errorf("can't connect to nexus server (%s): %s", s.Url, err.Error())
		Log(ErrorLevel, "server", err.Error())
		return err
	}

	// Login
	_, err = nc.Login(s.User, s.Pass)
	if err != nil {
		err = fmt.Errorf("can't login to nexus server (%s) as (%s): %s", s.Url, s.User, err.Error())
		Log(ErrorLevel, "server", err.Error())
		return err
	}

	// Configure services
	if s.services == nil || len(s.services) == 0 {
		err = fmt.Errorf("no services to serve")
		Log(ErrorLevel, "server", err.Error())
		return err
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
		Log(DebugLevel, "signal", "received SIGINT: stop gracefuly")
		for _, svc := range s.services {
			svc.GracefulStop()
		}
		<-signalChan
		Log(DebugLevel, "signal", "received SIGINT again: stop")
		for _, svc := range s.services {
			svc.Stop()
		}
	}()

	// Serve
	s.wg = &sync.WaitGroup{}
	errCh := make(chan error, 0)
	for _, svc := range s.services {
		s.wg.Add(1)
		go func(serv *Service) {
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
