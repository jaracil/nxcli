// Package service is boilerplate code for making nexus services.
package service

import (
	"fmt"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	nxcli "github.com/jaracil/nxcli"
	"github.com/jaracil/nxcli/demos/go/sugar/util"
	nexus "github.com/jaracil/nxcli/nxcore"
)

type Service struct {
	Url              string
	Prefix           string
	Pulls            int
	PullTimeout      time.Duration
	MaxThreads       int
	DebugEnabled     bool
	StatsPeriod      time.Duration
	GracefulExitTime time.Duration
	nc               *nexus.NexusConn
	methods          map[string]func(*nexus.Task)
	handler          func(*nexus.Task)
	stats            *Stats
	stopServeCh      chan (bool)
	threadsSem       *Semaphore
	wg               *sync.WaitGroup
	stopping         bool
}

type Stats struct {
	taskPullsDone       uint64
	taskPullTimeouts    uint64
	tasksPulled         uint64
	tasksPanic          uint64
	tasksServed         uint64
	tasksMethodNotFound uint64
	tasksRunning        uint64
}

// GetConn returns the underlying nexus connection
func (s *Service) GetConn() *nexus.NexusConn {
	return s.nc
}

// AddMethod adds (or replaces if already added) a method for the service
// The function that receives the nexus.Task should SendError() or SendResult() with it
func (s *Service) AddMethod(name string, f func(*nexus.Task)) {
	if s.methods == nil {
		s.methods = map[string]func(*nexus.Task){}
	}
	s.methods[name] = f
}

// SetHandler sets the task handler for all methods, to allow custom parsing of the method
// When a handler is set, methods added with AddMethod() have no effect
// Passing a nil will remove the handler and turn back to methods from AddMethod()
func (s *Service) SetHandler(h func(*nexus.Task)) {
	s.handler = h
}

// SetUrl modifies the service url
func (s *Service) SetUrl(url string) {
	s.Url = url
}

// SetPrefix modifies the service prefix
func (s *Service) SetPrefix(prefix string) {
	s.Prefix = prefix
}

// SetPulls modifies the number of concurrent nexus.TaskPull calls
func (s *Service) SetPulls(pulls int) {
	if pulls <= 0 {
		pulls = 1
	}
	s.Pulls = pulls
}

// SetMaxThreads modifies the number of maximum concurrent goroutines resolving nexus.Task
func (s *Service) SetMaxThreads(maxThreads int) {
	if maxThreads <= 0 {
		maxThreads = 1
	}
	s.MaxThreads = maxThreads
}

// SetPullTimeout modifies the time to wait for a nexus.Task for each nexus.TaskPull call
func (s *Service) SetPullTimeout(t time.Duration) {
	if t < 0 {
		t = 0
	}
	s.PullTimeout = t
}

// SetDebug enables debug messages
func (s *Service) SetDebugEnabled(t bool) {
	s.DebugEnabled = t
}

// SetDebugStatsPeriod changes the period for the stats to be printed
func (s *Service) SetDebugStatsPeriod(t time.Duration) {
	s.StatsPeriod = t
}

// SetGratefulExitTime sets the gracefull waiting time after a call to StopGraceful() is done
func (s *Service) SetGracefulExitTime(t time.Duration) {
	s.GracefulExitTime = t
}

// GetMethods returns a list of the methods the service has
// It returns nil if using a handler
func (s *Service) GetMethods() []string {
	if s.handler != nil {
		return nil
	}
	ms := []string{}
	for m, _ := range s.methods {
		ms = append(ms, m)
	}
	return ms
}

// GracefulStop stops pulling tasks, tries to finish working tasks and then cancels nexus connection (with a timeout)
func (s *Service) GracefulStop() {
	select {
	case s.stopServeCh <- true:
	default:
	}
}

// Stop cancels nexus connection, cancelling all tasks and stops serving
func (s *Service) Stop() {
	select {
	case s.stopServeCh <- false:
	default:
	}
}

// Serves connects to nexus, logins, and launches configured TaskPulls.
func (s *Service) Serve() error {
	var err error

	// Return an error if no methods where added
	if s.methods == nil && s.handler == nil {
		return fmt.Errorf("No methods to serve")
	}

	// Parse url
	parsed, err := url.Parse(s.Url)
	if err != nil {
		return fmt.Errorf("Invalid nexus url (%s): %s", s.Url, err.Error())
	}
	if parsed.User == nil {
		return fmt.Errorf("Invalid nexus url (%s): user or pass not found", s.Url)
	}
	username := parsed.User.Username()
	password, _ := parsed.User.Password()
	if username == "" || password == "" {
		return fmt.Errorf("Invalid nexus url (%s): user or pass is empty", s.Url)
	}

	// Check service
	if s.MaxThreads < 0 {
		s.MaxThreads = 1
	}
	if s.Pulls < 0 {
		s.Pulls = 1
	}
	if s.PullTimeout < 0 {
		s.PullTimeout = 0
	}
	if s.StatsPeriod < time.Millisecond*100 {
		s.StatsPeriod = time.Millisecond * 100
	}
	if s.GracefulExitTime <= time.Second {
		s.GracefulExitTime = time.Second
	}

	// Dial
	s.nc, err = nxcli.Dial(s.Url, nxcli.NewDialOptions())
	if err != nil {
		return fmt.Errorf("Can't connect to nexus server (%s): %s", s.Url, err.Error())
	}

	// Login
	_, err = s.nc.Login(username, password)
	if err != nil {
		return fmt.Errorf("Can't login to nexus server (%s)", s.Url)
	}

	// Output
	if s.DebugEnabled {
		log.Println(s)
	}

	// Serve
	s.wg = &sync.WaitGroup{}
	s.stopServeCh = make(chan (bool), 1)
	s.stats = &Stats{}
	s.stopping = false
	if s.threadsSem == nil {
		s.threadsSem = newSemaphore(s.MaxThreads)
	}
	for i := 1; i < s.Pulls+1; i++ {
		go s.taskPull(i)
	}

	// Wait until the nexus connection ends
	gracefulTimeout := &time.Timer{}
	wgDoneCh := make(chan (bool), 1)
	statsTicker := time.NewTicker(s.StatsPeriod)
	var graceful bool
	for {
		select {
		case <-statsTicker.C:
			if s.DebugEnabled {
				nst := s.GetStats()
				log.Printf("STATS: threads[ %d/%d ] task_pulls[ done=%d timeouts=%d ] tasks[ pulled=%d panic=%d errmethod=%d served=%d running=%d ]\n", s.threadsSem.Used(), s.threadsSem.Cap(), nst.taskPullsDone, nst.taskPullTimeouts, nst.tasksPulled, nst.tasksPanic, nst.tasksMethodNotFound, nst.tasksServed, nst.tasksRunning)
			}
		case graceful = <-s.stopServeCh: // Someone called Stop() or GracefulStop()
			if !s.stopping {
				s.stopping = true
				// Stop()
				if !graceful {
					s.nc.Cancel()
					gracefulTimeout = time.NewTimer(time.Second)
					continue
				}
				// GracefulStop()
				gracefulTimeout = time.NewTimer(s.GracefulExitTime)
				go func() {
					s.wg.Wait()
					wgDoneCh <- true
				}()
			}
		case <-wgDoneCh: // All workers finished
			s.nc.Cancel()
			continue
		case <-gracefulTimeout.C: // Graceful timeout
			if !graceful {
				return nil
			}
			s.nc.Cancel()
			return fmt.Errorf("Graceful stop: timeout after %s\n", s.GracefulExitTime.String())
		case <-s.nc.GetContext().Done(): // Nexus connection ended
			if s.stopping {
				if s.DebugEnabled {
					if graceful {
						log.Printf("Stopped gracefully")
					} else {
						log.Printf("Stopped")
					}
				}
				return nil
			}
			if ctxErr := s.nc.GetContext().Err(); ctxErr != nil {
				return fmt.Errorf("Nexus connection ended: stopped serving: %s", ctxErr.Error())
			}
			return fmt.Errorf("Nexus connection ended: stopped serving")
		}
	}
	return nil
}

func (s *Service) taskPull(n int) {
	for {
		if s.stopping {
			return
		}
		s.threadsSem.Acquire()
		atomic.AddUint64(&s.stats.taskPullsDone, 1)
		task, err := s.nc.TaskPull(s.Prefix, s.PullTimeout)
		if err != nil {
			if util.IsNexusErrCode(err, nexus.ErrTimeout) {
				atomic.AddUint64(&s.stats.taskPullTimeouts, 1)
				s.threadsSem.Release()
				continue
			}
			if s.DebugEnabled {
				log.Printf("Error pulling task on pull %d: %s\n", n, err.Error())
			}
			s.nc.Cancel()
			s.threadsSem.Release()
			return
		}
		atomic.AddUint64(&s.stats.tasksPulled, 1)
		if s.DebugEnabled {
			log.Printf("PULL %d: task[ path=%s method=%s params=%+v tags=%+v ]\n", n, task.Path, task.Method, task.Params, task.Tags)
		}

		f := s.handler
		if f == nil {
			var ok bool
			f, ok = s.methods[task.Method]
			if !ok {
				task.SendError(nexus.ErrMethodNotFound, "", nil)
				atomic.AddUint64(&s.stats.tasksMethodNotFound, 1)
				s.threadsSem.Release()
				continue
			}
		}

		// Execute the task
		go func() {
			defer s.threadsSem.Release()
			s.wg.Add(1)
			defer s.wg.Done()
			atomic.AddUint64(&s.stats.tasksRunning, 1)
			defer atomic.AddUint64(&s.stats.tasksRunning, ^uint64(0))
			defer func() {
				if r := recover(); r != nil {
					var nerr error
					var ok bool
					atomic.AddUint64(&s.stats.tasksPanic, 1)
					nerr, ok = r.(error)
					if !ok {
						nerr = fmt.Errorf("pkg: %v", r)
					}
					log.Printf("Panic serving task on pull %d: %s", n, nerr.Error())
					task.SendError(nexus.ErrInternal, nerr.Error(), nil)
				}
			}()
			f(task)
			atomic.AddUint64(&s.stats.tasksServed, 1)
		}()
	}
}

// GetStats returns the service stats
func (s *Service) GetStats() *Stats {
	return &Stats{
		taskPullsDone:       atomic.LoadUint64(&s.stats.taskPullsDone),
		taskPullTimeouts:    atomic.LoadUint64(&s.stats.taskPullTimeouts),
		tasksMethodNotFound: atomic.LoadUint64(&s.stats.tasksMethodNotFound),
		tasksPanic:          atomic.LoadUint64(&s.stats.tasksPanic),
		tasksPulled:         atomic.LoadUint64(&s.stats.tasksPulled),
		tasksServed:         atomic.LoadUint64(&s.stats.tasksServed),
		tasksRunning:        atomic.LoadUint64(&s.stats.tasksRunning),
	}
}

// String returns some service info as a stirng
func (s *Service) String() string {
	return fmt.Sprintf("SERVICE: config[ url=%s prefix=%s methods=%+v pulls=%d pullTimeout=%s maxThreads=%d ]", s.Url, s.Prefix, s.GetMethods(), s.Pulls, s.PullTimeout.String(), s.MaxThreads)
}
