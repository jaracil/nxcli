// Package service is boilerplate code for making nexus services.
package service

import (
	"fmt"
	"log"
	"net/url"
	"os"
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
	stats            *Stats
	stopServeCh      chan (bool)
	wg               *sync.WaitGroup
}

type Stats struct {
	taskPullsDone       uint64
	taskPullTimeouts    uint64
	tasksPulled         uint64
	tasksPanic          uint64
	tasksServed         uint64
	tasksMethodNotFound uint64
}

// Print returns a service as a string
func (s *Service) String() string {
	return fmt.Sprintf("SERVICE: config[ url=%s prefix=%s methods=%+v pulls=%d pullTimeout=%s maxThreads=%d ]", s.Url, s.Prefix, s.GetMethods(), s.Pulls, s.PullTimeout.String(), s.MaxThreads)
}

// AddMethod adds (or replaces if already added) a method for the service
// The function that receives the nexus.Task should SendError() or SendResult() with it
func (s *Service) AddMethod(name string, f func(*nexus.Task)) {
	if s.methods == nil {
		s.methods = map[string]func(*nexus.Task){}
	}
	s.methods[name] = f
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
func (s *Service) GetMethods() []string {
	ms := []string{}
	for m, _ := range s.methods {
		ms = append(ms, m)
	}
	return ms
}

// Serves connects to nexus, logins, and launches configured TaskPulls.
// When a
func (s *Service) Serve() error {
	var err error

	// Return an error if no methods where added
	if s.methods == nil {
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
	if s.StatsPeriod < time.Second {
		s.StatsPeriod = time.Second
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
	threadsSem := newSemaphore(s.MaxThreads)
	for i := 1; i < s.Pulls+1; i++ {
		go s.taskPull(i, threadsSem, s.stats)
	}

	// Wait
	statsTicker := time.NewTicker(s.StatsPeriod)
	for {
		select {
		case <-statsTicker.C:
			if s.DebugEnabled {
				nst := s.GetStats()
				log.Printf("STATS: threads[ %d/%d ] task_pulls[ done=%d timeouts=%d ] tasks[ pulled=%d panic=%d errmethod=%d served=%d ]\n", threadsSem.Used(), threadsSem.Cap(), nst.taskPullsDone, nst.taskPullTimeouts, nst.tasksPulled, nst.tasksPanic, nst.tasksMethodNotFound, nst.tasksServed)
			}
		case <-s.stopServeCh:
			s.nc.Cancel()
		case <-s.nc.GetContext().Done():
			s.wg.Wait()
			if s.DebugEnabled {
				log.Printf("Graceful stop: done")
			}
			return nil
		}
	}
	return nil
}

// GracefulStop cancels nexus connection and waits for it to end
func (s *Service) GracefulStop() {
	s.stopServeCh <- true
	go func() {
		select {
		case <-time.After(s.GracefulExitTime):
			if s.DebugEnabled {
				log.Printf("Graceful stop: timeout after %s\n", s.GracefulExitTime.String())
			}
			os.Exit(0)
		}
	}()
}

// Stop cancels nexus connection and exits
func (s *Service) Stop() {
	if s.DebugEnabled {
		log.Println("Stop: done")
	}
	s.nc.Cancel()
	os.Exit(0)
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
	}
}

func (s *Service) taskPull(n int, threadsSem *Semaphore, stats *Stats) {
	s.wg.Add(1)
	defer s.wg.Done()
	for {
		threadsSem.Acquire()
		atomic.AddUint64(&stats.taskPullsDone, 1)
		task, err := s.nc.TaskPull(s.Prefix, s.PullTimeout)
		if err != nil {
			if util.IsNexusErrCode(err, nexus.ErrTimeout) {
				atomic.AddUint64(&stats.taskPullTimeouts, 1)
				threadsSem.Release()
				continue
			}
			if s.DebugEnabled {
				log.Printf("Error pulling task on pull %d: %s\n", n, err.Error())
			}
			s.nc.Cancel()
			threadsSem.Release()
			return
		}
		atomic.AddUint64(&stats.tasksPulled, 1)
		if s.DebugEnabled {
			log.Printf("PULL %d: task[ path=%s method=%s params=%+v tags=%+v]\n", n, task.Path, task.Method, task.Params, task.Tags)
		}
		f, ok := s.methods[task.Method]
		if !ok {
			task.SendError(nexus.ErrMethodNotFound, "", nil)
			atomic.AddUint64(&stats.tasksMethodNotFound, 1)
			threadsSem.Release()
			continue
		}
		go func() {
			s.wg.Add(1)
			defer s.wg.Done()
			defer threadsSem.Release()
			defer func() {
				if r := recover(); r != nil {
					var nerr error
					var ok bool
					atomic.AddUint64(&stats.tasksPanic, 1)
					nerr, ok = r.(error)
					if !ok {
						nerr = fmt.Errorf("pkg: %v", r)
					}
					log.Printf("Panic serving task on pull %d: %s", n, nerr.Error())
					// return internal error?
					task.SendError(nexus.ErrInternal, nerr.Error(), nil)
				}
			}()

			f(task)
			atomic.AddUint64(&stats.tasksServed, 1)
		}()
	}
}
