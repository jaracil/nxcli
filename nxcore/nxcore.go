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
	Tags   map[string]interface{}
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
	Msgs    []*Msg `json:"Msgs"`    // Messages
	Waiting int    `json:"Waiting"` // Number of messages waiting in Nexus server since last read
	Drops   int    `json:"Drops"`   // Number of messages dropped (pipe overflows) since last read
}

// PipeOpts represents pipe creation options
type PipeOpts struct {
	Length int `json:"len,omitempty"` // Pipe buffer capacity
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

// TaskPush pushes a task to Nexus cloud.
// method is the method path Ex. "test.fibonacci.fib"
// params is the method params object.
// timeout is the maximum time waiting for response.
// Returns the task result or error.
func (nc *NexusConn) TaskPush(method string, params interface{}, timeout time.Duration) (interface{}, error) {
	par := map[string]interface{}{
		"method": method,
		"params": params,
	}
	if timeout > 0 {
		par["timeout"] = float64(timeout) / float64(time.Second)
	}
	return nc.Exec("task.push", par)
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
	task := &Task{
		nc:     nc,
		taskId: ei.N(res).M("taskid").StringZ(),
		Path:   ei.N(res).M("path").StringZ(),
		Method: ei.N(res).M("method").StringZ(),
		Params: ei.N(res).M("params").RawZ(),
		Tags:   ei.N(res).M("tags").MapStrZ(),
	}
	return task, nil
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
func (nc *NexusConn) PipeCreate(opts *PipeOpts) (*Pipe, error) {
	if opts == nil {
		opts = &PipeOpts{}
	}
	res, err := nc.Exec("pipe.create", opts)
	if err != nil {
		return nil, err
	}
	return nc.PipeOpen(ei.N(res).M("pipeid").StringZ())
}

// ChanSubscribe subscribes a pipe to a channel.
// Returns the response object from Nexus or error.
func (nc *NexusConn) ChanSubscribe(pipe *Pipe, channel string) (interface{}, error) {
	par := ei.M{
		"pipeid": pipe.Id(),
		"chan":   channel,
	}
	return nc.Exec("chan.sub", par)
}

// ChanUnsubscribe unsubscribes a pipe from a channel.
// Returns the response object from Nexus or error.
func (nc *NexusConn) ChanUnsubscribe(pipe *Pipe, channel string) (interface{}, error) {
	par := ei.M{
		"pipeid": pipe.Id(),
		"chan":   channel,
	}
	return nc.Exec("chan.unsub", par)
}

// ChanPublish publishes message to a channel.
// Returns the response object from Nexus or error.
func (nc *NexusConn) ChanPublish(channel string, msg interface{}) (interface{}, error) {
	par := ei.M{
		"chan": channel,
		"msg":  msg,
	}
	return nc.Exec("chan.pub", par)
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
