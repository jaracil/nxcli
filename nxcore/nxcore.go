package nxcore

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaracil/ei"
	"github.com/jaracil/smartio"
	"golang.org/x/net/context"
)

const (
	ErrParse            = -32700
	ErrInvalidRequest   = -32600
	ErrMethodNotFound   = -32601
	ErrInvalidParams    = -32602
	ErrInternal         = -32603
	ErrTimeout          = -32000
	ErrCancel           = -32001
	ErrInvalidTask      = -32002
	ErrInvalidPipe      = -32003
	ErrInvalidUser      = -32004
	ErrUserExists       = -32005
	ErrPermissionDenied = -32010
	ErrTtlExpired       = -32011
)

var ErrStr = map[int]string{
	ErrParse:            "Parse error",
	ErrInvalidRequest:   "Invalid request",
	ErrMethodNotFound:   "Method not found",
	ErrInvalidParams:    "Invalid params",
	ErrInternal:         "Internal error",
	ErrTimeout:          "Timeout",
	ErrCancel:           "Cancel",
	ErrInvalidTask:      "Invalid task",
	ErrInvalidPipe:      "Invalid pipe",
	ErrInvalidUser:      "Invalid user",
	ErrUserExists:       "User already exists",
	ErrPermissionDenied: "Permission denied",
	ErrTtlExpired:       "TTL expired",
}

type JsonRpcErr struct {
	Cod  int         `json:"code"`
	Mess string      `json:"message"`
	Dat  interface{} `json:"data,omitempty"`
}

func (e *JsonRpcErr) Error() string {
	return fmt.Sprintf("[%d] %s", e.Cod, e.Mess)
}

func (e *JsonRpcErr) Code() int {
	return e.Cod
}

func (e *JsonRpcErr) Data() interface{} {
	return e.Dat
}

// NewJsonRpcErr creates new JSON-RPC error.
//
// code is the JSON-RPC error code.
// message is optional in case of well known error code (negative values).
// data is an optional extra info object.
func NewJsonRpcErr(code int, message string, data interface{}) error {
	if code < 0 {
		if message != "" {
			message = fmt.Sprintf("%s:[%s]", ErrStr[code], message)
		} else {
			message = ErrStr[code]
		}
	}

	return &JsonRpcErr{Cod: code, Mess: message, Dat: data}
}

type JsonRpcReq struct {
	Jsonrpc string      `json:"jsonrpc"`
	Id      uint64      `json:"id,omitempty"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	nc      *NexusConn
}

type JsonRpcRes struct {
	Jsonrpc string      `json:"jsonrpc"`
	Id      uint64      `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *JsonRpcErr `json:"error,omitempty"`
}

// NexusConn represents the Nexus connection.
type NexusConn struct {
	conn         net.Conn
	connRx       *smartio.SmartReader
	connTx       *smartio.SmartWriter
	reqCount     uint64
	resTable     map[uint64]chan *JsonRpcRes
	resTableLock sync.Mutex
	chReq        chan *JsonRpcReq
	closed       int32 //Goroutine safe bool
	context      context.Context
	cancelFun    context.CancelFunc
	wdog         int64
}

// Task represents a task pushed to Nexus.
type Task struct {
	nc     *NexusConn
	taskId string
	Path   string
	Method string
	Params interface{}
	Prio   int
	Detach bool
	User   string
	Tags   map[string]interface{}
}

// TaskOpts represents task push options.
type TaskOpts struct {
	// Task priority default 0 (Set negative value for lower priority)
	Priority int
	// Task ttl default 5
	Ttl int
	// Task detach. If true, task is detached from creating session.
	// If task is detached and creating session deads, task is not removed from tasks queue.
	Detach bool
}

// Pipe represents a pipe.
// Pipes can only be read from the connection that created them and
// can be written from any conection.
type Pipe struct {
	nc        *NexusConn
	pipeId    string
	context   context.Context
	cancelFun context.CancelFunc
}

// Msg represents a pipe single message
type Msg struct {
	Count int64       // Message counter (unique and correlative)
	Msg   interface{} // Pipe message
}

// PipeData represents a pipe messages group obtained in read ops.
type PipeData struct {
	Msgs    []*Msg // Messages
	Waiting int    // Number of messages waiting in Nexus server since last read
	Drops   int    // Number of messages dropped (pipe overflows) since last read
}

// PipeOpts represents pipe creation options
type PipeOpts struct {
	Length int // Pipe buffer capacity
}

// NewNexusConn creates new nexus connection from net.conn
func NewNexusConn(conn net.Conn) *NexusConn {
	nc := &NexusConn{
		conn:     conn,
		connRx:   smartio.NewSmartReader(conn),
		connTx:   smartio.NewSmartWriter(conn),
		resTable: make(map[uint64]chan *JsonRpcRes),
		chReq:    make(chan *JsonRpcReq, 16),
		wdog:     60,
	}
	nc.context, nc.cancelFun = context.WithCancel(context.Background())
	go nc.sendWorker()
	go nc.recvWorker()
	go nc.mainWorker()
	return nc
}

func (nc *NexusConn) pushReq(req *JsonRpcReq) (err error) {
	select {
	case nc.chReq <- req:
	case <-nc.context.Done():
		err = NewJsonRpcErr(ErrCancel, "", nil)
	}
	return
}

func (nc *NexusConn) pullReq() (req *JsonRpcReq, err error) {
	select {
	case req = <-nc.chReq:
	case <-nc.context.Done():
		err = NewJsonRpcErr(ErrCancel, "", nil)
	}
	return
}

func (nc *NexusConn) mainWorker() {
	defer nc.Close()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			now := time.Now().Unix()
			wdog := atomic.LoadInt64(&nc.wdog)
			max := wdog * 2
			if (now-nc.connRx.GetLast() > max) &&
				(now-nc.connTx.GetLast() > max) {
				return
			}
			if (now-nc.connRx.GetLast() >= wdog) &&
				(now-nc.connTx.GetLast() >= wdog) {
				if err := nc.Ping(10 * time.Second); err != nil {
					if time.Now().Unix()-nc.connTx.GetLast() > 5 {
						return
					}
				}
			}
		case <-nc.context.Done():
			return
		}
	}
}

func (nc *NexusConn) sendWorker() {
	defer nc.Close()
	for {
		req, err := nc.pullReq()
		if err != nil {
			break
		}
		req.Jsonrpc = "2.0"
		buf, err := json.Marshal(req)
		if err != nil {
			break
		}
		if !nc.Closed() {
			_, err = nc.connTx.Write(buf)
			if err != nil {
				break
			}
		} else {
			break
		}
	}
}

func (nc *NexusConn) recvWorker() {
	defer nc.Close()
	dec := json.NewDecoder(nc.connRx)
	for dec.More() {
		res := &JsonRpcRes{}
		err := dec.Decode(res)
		if err != nil {
			break
		}
		nc.resTableLock.Lock()
		ch := nc.resTable[res.Id]
		nc.resTableLock.Unlock()
		if ch != nil {
			select {
			case ch <- res:
			default:
			}
		}
	}
}

func (nc *NexusConn) newId() (uint64, chan *JsonRpcRes) {
	id := atomic.AddUint64(&nc.reqCount, 1)
	ch := make(chan *JsonRpcRes, 1)
	nc.resTableLock.Lock()
	defer nc.resTableLock.Unlock()
	nc.resTable[id] = ch
	return id, ch
}

func (nc *NexusConn) delId(id uint64) {
	nc.resTableLock.Lock()
	delete(nc.resTable, id)
	nc.resTableLock.Unlock()
}

// GetContext returns internal connection context.
func (nc *NexusConn) GetContext() context.Context {
	return nc.context
}

// Close closes nexus connection.
func (nc *NexusConn) Close() {
	if atomic.CompareAndSwapInt32(&nc.closed, 0, 1) {
		nc.cancelFun()
		nc.conn.Close()
	}
}

// Closed returns Nexus connection state.
func (nc *NexusConn) Closed() bool {
	return atomic.LoadInt32(&nc.closed) == 1
}

// ExecNoWait is a low level JSON-RPC call function, it don't wait response from server.
func (nc *NexusConn) ExecNoWait(method string, params interface{}) (id uint64, rch chan *JsonRpcRes, err error) {
	if nc.Closed() {
		err = NewJsonRpcErr(ErrCancel, "", nil)
		return
	}
	id, rch = nc.newId()
	req := &JsonRpcReq{
		Id:     id,
		Method: method,
		Params: params,
	}
	err = nc.pushReq(req)
	if err != nil {
		nc.delId(id)
		return 0, nil, err
	}
	return
}

// Exec is a low level JSON-RPC call function.
func (nc *NexusConn) Exec(method string, params interface{}) (result interface{}, err error) {
	id, rch, err := nc.ExecNoWait(method, params)
	if err != nil {
		return nil, err
	}
	defer nc.delId(id)
	select {
	case res := <-rch:
		if res.Error != nil {
			err = res.Error
		} else {
			result = res.Result
		}
	case <-nc.context.Done():
		err = NewJsonRpcErr(ErrCancel, "", nil)
	}
	return
}

// Ping pings Nexus server, timeout is the max time waiting for server response,
// after that ErrTimeout is returned.
func (nc *NexusConn) Ping(timeout time.Duration) (err error) {
	id, rch, err := nc.ExecNoWait("sys.ping", nil)
	if err != nil {
		return
	}
	defer nc.delId(id)
	select {
	case <-rch:
		break
	case <-time.After(timeout):
		err = NewJsonRpcErr(ErrTimeout, "", nil)
	case <-nc.context.Done():
		err = NewJsonRpcErr(ErrCancel, "", nil)
	}
	return
}

// Login attempts to login using user and pass.
// Returns the response object from Nexus or error.
func (nc *NexusConn) Login(user string, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	return nc.Exec("sys.login", par)

}

type UserSessions struct {
	User     string        `json:"user"`
	Sessions []SessionInfo `json:"sessions"`
	N        int           `json:"n"`
}

type SessionInfo struct {
	Id            string    `json:"id"`
	NodeId        string    `json:"nodeId"`
	RemoteAddress string    `json:"remoteAddress"`
	Protocol      string    `json:"protocol"`
	CreationTime  time.Time `json:"creationTime"`
}

// Sessions returns info of the users sessions
// Returns a list of SessionInfo structs or an error
func (nc *NexusConn) SessionList(prefix string) ([]UserSessions, error) {
	par := map[string]interface{}{
		"prefix": prefix,
	}
	res, err := nc.Exec("sys.sessions.list", par)
	if err != nil {
		return nil, err
	}
	sessions := make([]UserSessions, 0)
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &sessions)
	if err != nil {
		return nil, err
	}

	return sessions, nil
}

// SessionKick forces the node owner of the client connection to close it
// Returns the response object from Nexus or error.
func (nc *NexusConn) SessionKick(connId string) (interface{}, error) {
	par := map[string]interface{}{
		"connId": connId,
	}
	return nc.Exec("sys.sessions.kick", par)
}

type NodeInfo struct {
	Load    map[string]float64 `json:"load"`
	Clients int                `json:"clients"`
	NodeId  string             `json:"id"`
}

// Nodes returns info of the nodes state
// Returns a list of NodeInfo structs or an error
func (nc *NexusConn) NodeList() ([]NodeInfo, error) {
	par := map[string]interface{}{}
	res, err := nc.Exec("sys.nodes.list", par)
	if err != nil {
		return nil, err
	}
	nodes := make([]NodeInfo, 0)
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &nodes)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// TaskPush pushes a task to Nexus cloud.
// method is the method path Ex. "test.fibonacci.fib"
// params is the method params object.
// timeout is the maximum time waiting for response, 0 = no timeout.
// options (see TaskOpts struct)
// Returns the task result or error.
func (nc *NexusConn) TaskPush(method string, params interface{}, timeout time.Duration, opts ...*TaskOpts) (interface{}, error) {
	par := ei.M{
		"method": method,
		"params": params,
	}
	if len(opts) > 0 {
		if opts[0].Priority != 0 {
			par["prio"] = opts[0].Priority
		}
		if opts[0].Ttl != 0 {
			par["ttl"] = opts[0].Ttl
		}
		if opts[0].Detach {
			par["detach"] = true
		}
	}
	if timeout > 0 {
		par["timeout"] = float64(timeout) / float64(time.Second)
	}
	return nc.Exec("task.push", par)
}

// TaskPushCh pushes a task to Nexus cloud.
// method is the method path Ex. "test.fibonacci.fib"
// params is the method params object.
// timeout is the maximum time waiting for response, 0 = no timeout.
// options (see TaskOpts struct)
// Returns two channels (one for result of interface{} type and one for error of error type).
func (nc *NexusConn) TaskPushCh(method string, params interface{}, timeout time.Duration, opts ...*TaskOpts) (<-chan interface{}, <-chan error) {
	chres := make(chan interface{}, 1)
	cherr := make(chan error, 1)
	go func() {
		res, err := nc.TaskPush(method, params, timeout, opts...)
		if err != nil {
			cherr <- err
		} else {
			chres <- res
		}

	}()
	return chres, cherr
}

// TaskPull pulls a task from Nexus cloud.
// prefix is the method prefix we want pull Ex. "test.fibonacci"
// timeout is the maximum time waiting for a task.
// Returns a new incomming Task or error.
func (nc *NexusConn) TaskPull(prefix string, timeout time.Duration) (*Task, error) {
	par := map[string]interface{}{
		"prefix": prefix,
	}
	if timeout > 0 {
		par["timeout"] = float64(timeout) / float64(time.Second)
	}
	res, err := nc.Exec("task.pull", par)
	if err != nil {
		return nil, err
	}
	t := ei.N(res)
	task := &Task{
		nc:     nc,
		taskId: t.M("taskid").StringZ(),
		Path:   t.M("path").StringZ(),
		Method: t.M("method").StringZ(),
		Params: t.M("params").RawZ(),
		Tags:   t.M("tags").MapStrZ(),
		Prio:   t.M("prio").IntZ(),
		Detach: t.M("detach").BoolZ(),
		User:   t.M("user").StringZ(),
	}
	return task, nil
}

type TaskList struct {
	Pulls  map[string]int `json:"pulls"`
	Pushes map[string]int `json:"pushes"`
}

// TaskList returns how many push/pulls are happening on a path
// prefix is the method prefix we want pull Ex. "test.fibonacci"
// Returns a TaskList or error.
func (nc *NexusConn) TaskList(prefix string) (*TaskList, error) {
	par := map[string]interface{}{
		"prefix": prefix,
	}
	res, err := nc.Exec("task.list", par)
	if err != nil {
		return nil, err
	}
	list := &TaskList{}
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, list)
	if err != nil {
		return nil, err
	}

	return list, nil
}

// UserCreate creates new user in Nexus user's table.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserCreate(user, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	return nc.Exec("user.create", par)
}

// UserDelete removes user from Nexus user's table.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserDelete(user string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
	}
	return nc.Exec("user.delete", par)
}

type UserInfo struct {
	User string                            `json:"user"`
	Tags map[string]map[string]interface{} `json:"tags"`
}

// UserList lists users from Nexus user's table.
// Returns a list of UserInfo or error.
func (nc *NexusConn) UserList(prefix string) ([]UserInfo, error) {
	par := map[string]interface{}{
		"prefix": prefix,
	}
	res, err := nc.Exec("user.list", par)
	if err != nil {
		return nil, err
	}
	users := make([]UserInfo, 0)
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &users)
	if err != nil {
		return nil, err
	}

	return users, nil
}

// UserSetTags set tags on user's prefix.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserSetTags(user string, prefix string, tags map[string]interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"user":   user,
		"prefix": prefix,
		"tags":   tags,
	}
	return nc.Exec("user.setTags", par)
}

// UserDelTags remove tags from user's prefix.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserDelTags(user string, prefix string, tags []string) (interface{}, error) {
	par := map[string]interface{}{
		"user":   user,
		"prefix": prefix,
		"tags":   tags,
	}
	return nc.Exec("user.delTags", par)
}

// UserSetPass sets new user password.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserSetPass(user string, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	return nc.Exec("user.setPass", par)
}

// PipeOpen Creates a new pipe from pipe identification string.
// Returns the new pipe object or error.
func (nc *NexusConn) PipeOpen(pipeId string) (*Pipe, error) {
	pipe := &Pipe{
		nc:     nc,
		pipeId: pipeId,
	}
	pipe.context, pipe.cancelFun = context.WithCancel(nc.context)
	return pipe, nil
}

// PipeCreate creates a new pipe.
// Returns the new pipe object or error.
func (nc *NexusConn) PipeCreate(opts ...*PipeOpts) (*Pipe, error) {
	par := ei.M{}
	if len(opts) > 0 {
		if opts[0].Length > 0 {
			par["len"] = opts[0].Length
		}
	}
	res, err := nc.Exec("pipe.create", par)
	if err != nil {
		return nil, err
	}
	return nc.PipeOpen(ei.N(res).M("pipeid").StringZ())
}

// TopicSubscribe subscribes a pipe to a topic.
// Returns the response object from Nexus or error.
func (nc *NexusConn) TopicSubscribe(pipe *Pipe, topic string) (interface{}, error) {
	par := ei.M{
		"pipeid": pipe.Id(),
		"topic":  topic,
	}
	return nc.Exec("topic.sub", par)
}

// TopicUnsubscribe unsubscribes a pipe from a topic.
// Returns the response object from Nexus or error.
func (nc *NexusConn) TopicUnsubscribe(pipe *Pipe, topic string) (interface{}, error) {
	par := ei.M{
		"pipeid": pipe.Id(),
		"topic":  topic,
	}
	return nc.Exec("topic.unsub", par)
}

// TopicPublish publishes message to a topic.
// Returns the response object from Nexus or error.
func (nc *NexusConn) TopicPublish(topic string, msg interface{}) (interface{}, error) {
	par := ei.M{
		"topic": topic,
		"msg":   msg,
	}
	return nc.Exec("topic.pub", par)
}

// Lock tries to get a lock.
// Returns lock success/failure or error.
func (nc *NexusConn) Lock(lock string) (bool, error) {
	par := ei.M{
		"lock": lock,
	}
	res, err := nc.Exec("sync.lock", par)
	if err != nil {
		return false, err
	}
	return ei.N(res).M("ok").BoolZ(), nil
}

// Unlock tries to free a lock.
// Returns unlock success/failure or error.
func (nc *NexusConn) Unlock(lock string) (bool, error) {
	par := ei.M{
		"lock": lock,
	}
	res, err := nc.Exec("sync.unlock", par)
	if err != nil {
		return false, err
	}
	return ei.N(res).M("ok").BoolZ(), nil
}

// Close closes pipe.
// Returns the response object from Nexus or error.
func (p *Pipe) Close() (interface{}, error) {
	p.cancelFun()
	par := map[string]interface{}{
		"pipeid": p.pipeId,
	}
	return p.nc.Exec("pipe.close", par)
}

// Write writes message to pipe.
// Returns the response object from Nexus or error.
func (p *Pipe) Write(msg interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"pipeid": p.pipeId,
		"msg":    msg,
	}
	return p.nc.Exec("pipe.write", par)
}

// Read reads up to (max) messages from pipe or until timeout occurs.
func (p *Pipe) Read(max int, timeout time.Duration) (*PipeData, error) {
	par := map[string]interface{}{
		"pipeid":  p.pipeId,
		"max":     max,
		"timeout": float64(timeout) / float64(time.Second),
	}
	res, err := p.nc.Exec("pipe.read", par)
	if err != nil {
		return nil, err
	}

	msgres := make([]*Msg, 0, 10)
	waiting := ei.N(res).M("waiting").IntZ()
	drops := ei.N(res).M("drops").IntZ()
	messages, ok := ei.N(res).M("msgs").RawZ().([]interface{})
	if !ok {
		return nil, NewJsonRpcErr(ErrInternal, "", nil)
	}

	for _, msg := range messages {
		m := &Msg{
			Count: ei.N(msg).M("count").Int64Z(),
			Msg:   ei.N(msg).M("msg").RawZ(),
		}

		msgres = append(msgres, m)
	}

	return &PipeData{Msgs: msgres, Waiting: waiting, Drops: drops}, nil
}

// Listen returns a pipe reader channel.
// ch is the channel used and returned by Listen, if ch is nil Listen creates a new unbuffered channel.
// channel is closed when pipe is closed or error happens.
func (p *Pipe) Listen(ch chan *Msg) chan *Msg {
	if ch == nil {
		ch = make(chan *Msg)
	}
	go func() {
		for {
			data, err := p.Read(100000, 0)
			if err != nil {
				close(ch)
				return
			}
			for _, msg := range data.Msgs {
				select {
				case ch <- msg:
				case <-p.context.Done():
					close(ch)
					return
				}
			}
		}
	}()
	return ch
}

// Id returns the pipe identification strring.
func (p *Pipe) Id() string {
	return p.pipeId
}

// SendResult closes Task with result.
// Returns the response object from Nexus or error.
func (t *Task) SendResult(res interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.taskId,
		"result": res,
	}
	return t.nc.Exec("task.result", par)
}

// SendError closes Task with error.
// code is the JSON-RPC error code.
// message is optional in case of well known error code (negative values).
// data is an optional extra info object.
// Returns the response object from Nexus or error.
func (t *Task) SendError(code int, message string, data interface{}) (interface{}, error) {
	if code < 0 {
		if message != "" {
			message = fmt.Sprintf("%s:[%s]", ErrStr[code], message)
		} else {
			message = ErrStr[code]
		}
	}
	par := map[string]interface{}{
		"taskid":  t.taskId,
		"code":    code,
		"message": message,
		"data":    data,
	}
	return t.nc.Exec("task.error", par)
}

// Reject rejects the task. Task is returned to Nexus tasks queue.
func (t *Task) Reject() (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.taskId,
	}
	return t.nc.Exec("task.reject", par)
}

// Accept accepts a detached task. Is an alias for SendResult(nil).
func (t *Task) Accept() (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.taskId,
		"result": nil,
	}
	return t.nc.Exec("task.result", par)
}

// GetConn retrieves the task underlying nexus connection.
func (t *Task) GetConn() *NexusConn {
	return t.nc
}
