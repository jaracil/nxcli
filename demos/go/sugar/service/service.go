// Package service is boilerplate code for making nexus services.
package service

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xeipuuv/gojsonschema"

	nxcli "github.com/jaracil/nxcli"
	. "github.com/jaracil/nxcli/demos/go/sugar/log"
	"github.com/jaracil/nxcli/demos/go/sugar/util"
	nexus "github.com/jaracil/nxcli/nxcore"
)

type Service struct {
	Name             string
	Server           string
	User             string
	Password         string
	Prefix           string
	Pulls            int
	PullTimeout      time.Duration
	MaxThreads       int
	StatsPeriod      time.Duration
	GracefulExitTime time.Duration
	LogLevel         string
	nc               *nexus.NexusConn
	methods          map[string]*Method
	handler          *Method
	stats            *Stats
	stopServeCh      chan (bool)
	threadsSem       *Semaphore
	wg               *sync.WaitGroup
	stopping         bool
	stopLock         *sync.Mutex
	debugEnabled     bool
	sharedConn       bool
}

type Method struct {
	schemaSource    string
	schema          interface{}
	schemaValidator *gojsonschema.Schema
	f               func(t *nexus.Task)
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

// SetConn sets the underlying nexus connection.
// Once SetConn is called, service url, user and password are ignored and the
// provided connection is used on serve.
func (s *Service) SetConn(nc *nexus.NexusConn) {
	s.nc = nc
	s.sharedConn = true
}

func (s *Service) addMethod(name string, schema string, f func(*nexus.Task) (interface{}, *nexus.JsonRpcErr)) {
	if s.methods == nil {
		s.methods = map[string]*Method{}
		s.methods["@schema"] = &Method{
			schemaSource:    "",
			schema:          nil,
			schemaValidator: nil,
			f: func(t *nexus.Task) {
				r := map[string]interface{}{}
				for name, m := range s.methods {
					if m.schema != nil {
						r[name] = m.schema
					}
				}
				t.SendResult(r)
			},
		}
	}
	s.methods[name] = &Method{schemaSource: "", schema: nil, schemaValidator: nil, f: defMethodWrapper(f)}
	if schema != "" {
		s.methods[name].schemaSource = schema
	}
}

func defMethodWrapper(f func(*nexus.Task) (interface{}, *nexus.JsonRpcErr)) func(*nexus.Task) {
	return func(t *nexus.Task) {
		res, err := f(t)
		if _, ok := t.Tags["@local@repliedTo"]; ok {
			return
		}
		if err != nil {
			t.SendError(err.Cod, err.Mess, err.Dat)
		} else {
			t.SendResult(res)
		}
	}
}

// AddMethod adds (or replaces if already added) a method for the service
// The function that receives the nexus.Task should return a result or an error
func (s *Service) AddMethod(name string, f func(*nexus.Task) (interface{}, *nexus.JsonRpcErr)) {
	s.addMethod(name, "", f)
}

// AddSchemaMethod adds (or replaces if already added) a method for the service with a JSON schema
// The function that receives the nexus.Task should return a result or an error
// If the schema validation does not succeed, an ErrInvalidParams error will be sent as a result for the task
func (s *Service) AddMethodSchema(name string, schema string, f func(*nexus.Task) (interface{}, *nexus.JsonRpcErr)) {
	s.addMethod(name, schema, f)
}

// SetHandler sets the task handler for all methods, to allow custom parsing of the method
// When a handler is set, methods added with AddMethod() have no effect
// Passing a nil will remove the handler and turn back to methods from AddMethod()
func (s *Service) SetHandler(h func(*nexus.Task) (interface{}, *nexus.JsonRpcErr)) {
	s.handler = &Method{schemaSource: "", schema: nil, schemaValidator: nil, f: defMethodWrapper(h)}
}

// SetUrl modifies the service url
func (s *Service) SetUrl(url string) {
	s.Server = url
}

// SetUser modifies the service user
func (s *Service) SetUser(user string) {
	s.User = user
}

// SetPass modifies the service pass
func (s *Service) SetPass(pass string) {
	s.Password = pass
}

// SetPrefix modifies the service prefix
func (s *Service) SetPrefix(prefix string) {
	s.Prefix = prefix
}

// SetPulls modifies the number of concurrent nexus.TaskPull calls
func (s *Service) SetPulls(pulls int) {
	s.Pulls = pulls
}

// SetMaxThreads modifies the number of maximum concurrent goroutines resolving nexus.Task
func (s *Service) SetMaxThreads(maxThreads int) {
	s.MaxThreads = maxThreads
}

// SetPullTimeout modifies the time to wait for a nexus.Task for each nexus.TaskPull call
func (s *Service) SetPullTimeout(t time.Duration) {
	s.PullTimeout = t
}

// SetLogLevel sets the log level
func (s *Service) SetLogLevel(t string) {
	t = strings.ToLower(t)
	SetLogLevel(t)
	if Log.Level.String() == t {
		s.LogLevel = t
		s.debugEnabled = t == "debug"
	}
}

// SetStatsPeriod changes the period for the stats to be printed
func (s *Service) SetStatsPeriod(t time.Duration) {
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

	// Set log name
	logn := s.Name
	if logn == "" {
		logn = "service"
	}
	logn = logn + ": "

	svcn := s.Name
	if svcn != "" {
		svcn = svcn + ": "
	}

	// Set log level
	s.SetLogLevel(s.LogLevel)

	// Return an error if no methods where added
	if s.methods == nil && s.handler == nil {
		return fmt.Errorf("%sno methods to serve", logn)
	}

	// Parse url
	if !s.sharedConn {
		_, err = url.Parse(s.Server)
		if err != nil {
			return fmt.Errorf("%sinvalid nexus url (%s): %s", logn, s.Server, err.Error())
		}
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
		if s.StatsPeriod < 0 {
			s.StatsPeriod = 0
		} else {
			s.StatsPeriod = time.Millisecond * 100
		}
	}
	if s.GracefulExitTime <= time.Second {
		s.GracefulExitTime = time.Second
	}

	// Check/create method schemas
	if s.handler == nil {
		for mname, m := range s.methods {
			if m.schemaSource != "" {
				var schres interface{}
				err = json.Unmarshal([]byte(m.schemaSource), &schres)
				if err != nil {
					return fmt.Errorf("%serror parsing method (%s) schema: %s", logn, mname, err.Error())
				}
				m.schemaValidator, err = gojsonschema.NewSchema(gojsonschema.NewGoLoader(schres))
				if err != nil {
					return fmt.Errorf("%serror on method (%s) schema: %s", logn, mname, err.Error())
				}
				m.schema = schres
			}
		}
	}

	if !s.sharedConn {
		// Dial
		s.nc, err = nxcli.Dial(s.Server, nxcli.NewDialOptions())
		if err != nil {
			return fmt.Errorf("%scan't connect to nexus server (%s): %s", logn, s.Server, err.Error())
		}

		// Login
		_, err = s.nc.Login(s.User, s.Password)
		if err != nil {
			return fmt.Errorf("%scan't login to nexus server (%s) as (%s): %s", logn, s.Server, s.User, err.Error())
		}
	}

	// Output
	Log.Infof("%s%s", logn, s)

	// Serve
	s.wg = &sync.WaitGroup{}
	s.stopServeCh = make(chan (bool), 1)
	s.stats = &Stats{}
	s.stopping = false
	s.stopLock = &sync.Mutex{}
	if s.threadsSem == nil {
		s.threadsSem = newSemaphore(s.MaxThreads)
	}
	for i := 1; i < s.Pulls+1; i++ {
		go s.taskPull(i, svcn)
	}

	// Wait for signals
	if !s.sharedConn {
		go func() {
			signalChan := make(chan os.Signal, 1)
			signal.Notify(signalChan, os.Interrupt)
			<-signalChan
			Log.Debugf("%ssignal: received Ctrl+C: stop gracefuly", svcn)
			s.GracefulStop()
			<-signalChan
			Log.Debugf("%ssignal: received Ctrl+C again: stop", svcn)
			s.Stop()
		}()
	}

	// Wait until the nexus connection ends
	gracefulTimeout := &time.Timer{}
	wgDoneCh := make(chan (bool), 1)
	var statsTicker *time.Ticker
	if s.StatsPeriod > 0 {
		statsTicker = time.NewTicker(s.StatsPeriod)
	}
	var graceful bool
	for {
		select {
		case <-statsTicker.C:
			if s.debugEnabled {
				nst := s.GetStats()
				Log.Debugf("%sstats: threads[ %d/%d ] task_pulls[ done=%d timeouts=%d ] tasks[ pulled=%d panic=%d errmethod=%d served=%d running=%d ]", svcn, s.threadsSem.Used(), s.threadsSem.Cap(), nst.taskPullsDone, nst.taskPullTimeouts, nst.tasksPulled, nst.tasksPanic, nst.tasksMethodNotFound, nst.tasksServed, nst.tasksRunning)
			}
		case graceful = <-s.stopServeCh: // Someone called Stop() or GracefulStop()
			if !graceful {
				s.nc.Close()
				gracefulTimeout = time.NewTimer(time.Second)
				s.setStopping()
				continue
			}
			if !s.isStopping() {
				s.setStopping()
				gracefulTimeout = time.NewTimer(s.GracefulExitTime)
				go func() {
					s.wg.Wait()
					wgDoneCh <- true
				}()
			}
		case <-wgDoneCh: // All workers finished
			s.nc.Close()
			continue
		case <-gracefulTimeout.C: // Graceful timeout
			if !graceful {
				Log.Debugf("%sstop: done", svcn)
				return nil
			}
			s.nc.Close()
			return fmt.Errorf("%sgraceful: timeout after %s", svcn, s.GracefulExitTime.String())
		case <-s.nc.GetContext().Done(): // Nexus connection ended
			if s.isStopping() {
				if graceful {
					Log.Debugf("%sgraceful: done", svcn)
				} else {
					Log.Debugf("%sstop: done", svcn)
				}
				return nil
			}
			if ctxErr := s.nc.GetContext().Err(); ctxErr != nil {
				return fmt.Errorf("%sstop: nexus connection ended: %s", ctxErr.Error(), svcn)
			}
			return fmt.Errorf("%sstop: nexus connection ended: stopped serving", svcn)
		}
	}
	return nil
}

func (s *Service) taskPull(n int, svcn string) {
	for {
		// Exit if stopping serve
		if s.isStopping() {
			return
		}

		// Make a task pull
		s.threadsSem.Acquire()
		atomic.AddUint64(&s.stats.taskPullsDone, 1)
		task, err := s.nc.TaskPull(s.Prefix, s.PullTimeout)
		if err != nil {
			if util.IsNexusErrCode(err, nexus.ErrTimeout) { // A timeout ocurred: pull again
				atomic.AddUint64(&s.stats.taskPullTimeouts, 1)
				s.threadsSem.Release()
				continue
			}
			if !s.isStopping() || !util.IsNexusErrCode(err, nexus.ErrCancel) { // An error ocurred (bypass if cancelled because service stop)
				Log.Errorf("%spull %d: pulling task: %s", svcn, n, err.Error())
			}
			s.nc.Close()
			s.threadsSem.Release()
			return
		}

		// A task has been pulled
		atomic.AddUint64(&s.stats.tasksPulled, 1)
		Log.Debugf("%spull %d: task[ path=%s method=%s params=%+v tags=%+v ]", svcn, n, task.Path, task.Method, task.Params, task.Tags)

		// Get method or global handler
		m := s.handler
		if m == nil {
			var ok bool
			m, ok = s.methods[task.Method]
			if !ok { // Method not found
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
					Log.Errorf("%spull %d: panic serving task: %s", svcn, n, nerr.Error())
					task.SendError(nexus.ErrInternal, nerr.Error(), nil)
				}
			}()

			// Validate schema
			if m.schema != nil {
				result, err := m.schemaValidator.Validate(gojsonschema.NewGoLoader(task.Params))
				if err != nil { // Error with schemas
					task.SendError(nexus.ErrInvalidParams, fmt.Sprintf("json schema validation: %s", err.Error()), nil)
				} else {
					if result.Valid() { // Schema validated: execute task
						m.f(task)
					} else { // Schema validation error
						out := "json schema validation: "
						nerrs := len(result.Errors())
						if nerrs == 1 {
							out += fmt.Sprintf("%s", result.Errors()[0])
						} else {
							for _, desc := range result.Errors() {
								out += fmt.Sprintf("\n- %s", desc)
							}
						}
						task.SendError(nexus.ErrInvalidParams, out, nil)
					}
				}
			} else { // No schema: execute task
				m.f(task)
			}
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
	if s.sharedConn {
		return fmt.Sprintf("config[ conn=shared url=%s prefix=%s methods=%+v pulls=%d pullTimeout=%s maxThreads=%d logLevel=%s statsPeriod=%s gracefulExit=%s ]", s.Server, s.Prefix, s.GetMethods(), s.Pulls, s.PullTimeout.String(), s.MaxThreads, s.LogLevel, s.StatsPeriod.String(), s.GracefulExitTime.String())
	}
	return fmt.Sprintf("config[ url=%s prefix=%s methods=%+v pulls=%d pullTimeout=%s maxThreads=%d logLevel=%s statsPeriod=%s gracefulExit=%s ]", s.Server, s.Prefix, s.GetMethods(), s.Pulls, s.PullTimeout.String(), s.MaxThreads, s.LogLevel, s.StatsPeriod.String(), s.GracefulExitTime.String())
}

func (s *Service) isStopping() bool {
	s.stopLock.Lock()
	defer s.stopLock.Unlock()
	return s.stopping
}

func (s *Service) setStopping() {
	s.stopLock.Lock()
	defer s.stopLock.Unlock()
	s.stopping = true
}
