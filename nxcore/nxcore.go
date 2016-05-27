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

type NexusConn struct {
	conn         net.Conn
	connRx       *smartio.SmartReader
	connTx       *smartio.SmartWriter
	reqCount     uint64
	resTable     map[uint64]chan *JsonRpcRes
	resTableLock sync.Mutex
	chReq        chan *JsonRpcReq
	context      context.Context
	cancelFun    context.CancelFunc
	wdog         int64
}

type Task struct {
	nc     *NexusConn
	taskId string
	Path   string
	Method string
	Params interface{}
	Tags   map[string]interface{}
}

type Pipe struct {
	nc     *NexusConn
	pipeId string
}

type Msg struct {
	Count int64
	Msg   interface{}
}

type PipeData struct {
	Msgs    []*Msg
	Waiting int
	Drops   int
}

type PipeOpts struct {
	Length int `json:"len,omitempty"`
}

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
	defer nc.conn.Close()
	defer nc.Cancel()
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
	defer nc.Cancel()
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
		_, err = nc.connTx.Write(buf)
		if err != nil {
			break
		}
	}
}

func (nc *NexusConn) recvWorker() {
	defer nc.Cancel()
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

func (nc *NexusConn) Cancel() {
	nc.cancelFun()
}

func (nc *NexusConn) Closed() bool {
	return nc.context.Err() != nil
}

func (nc *NexusConn) ExecNoWait(method string, params interface{}) (id uint64, rch chan *JsonRpcRes, err error) {
	if nc.context.Err() != nil {
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

func (nc *NexusConn) Login(user string, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	return nc.Exec("sys.login", par)

}

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

func (nc *NexusConn) UserCreate(user, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	return nc.Exec("user.create", par)
}

func (nc *NexusConn) UserDelete(user string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
	}
	return nc.Exec("user.delete", par)
}

func (nc *NexusConn) UserSetTags(user string, prefix string, tags map[string]interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"user":   user,
		"prefix": prefix,
		"tags":   tags,
	}
	return nc.Exec("user.setTags", par)
}

func (nc *NexusConn) UserDelTags(user string, prefix string, tags []string) (interface{}, error) {
	par := map[string]interface{}{
		"user":   user,
		"prefix": prefix,
		"tags":   tags,
	}
	return nc.Exec("user.delTags", par)
}

func (nc *NexusConn) UserSetPass(user string, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	return nc.Exec("user.setPass", par)
}

func (nc *NexusConn) PipeOpen(pipeId string) (*Pipe, error) {
	pipe := &Pipe{
		nc:     nc,
		pipeId: pipeId,
	}
	return pipe, nil
}

func (nc *NexusConn) PipeCreate(opts *PipeOpts) (*Pipe, error) {
	if opts == nil {
		opts = &PipeOpts{}
	}
	res, err := nc.Exec("pipe.create", opts)
	if err != nil {
		return nil, err
	}
	pipe := &Pipe{
		nc:     nc,
		pipeId: ei.N(res).M("pipeid").StringZ(),
	}
	return pipe, nil
}

func (nc *NexusConn) ChanSubscribe(pipe *Pipe, channel string) (interface{}, error) {
	par := ei.M{
		"pipeid": pipe.Id(),
		"chan":   channel,
	}
	return nc.Exec("chan.sub", par)
}

func (nc *NexusConn) ChanUnsubscribe(pipe *Pipe, channel string) (interface{}, error) {
	par := ei.M{
		"pipeid": pipe.Id(),
		"chan":   channel,
	}
	return nc.Exec("chan.unsub", par)
}

func (nc *NexusConn) ChanPublish(channel string, msg interface{}) (interface{}, error) {
	par := ei.M{
		"chan": channel,
		"msg":  msg,
	}
	return nc.Exec("chan.pub", par)
}

func (p *Pipe) Close() (interface{}, error) {
	par := map[string]interface{}{
		"pipeid": p.pipeId,
	}
	return p.nc.Exec("pipe.close", par)
}

func (p *Pipe) Write(msg interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"pipeid": p.pipeId,
		"msg":    msg,
	}
	return p.nc.Exec("pipe.write", par)
}

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

func (p *Pipe) Id() string {
	return p.pipeId
}

func (t *Task) SendResult(res interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.taskId,
		"result": res,
	}
	return t.nc.Exec("task.result", par)
}

func (t *Task) SendError(code int, message string, data interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"taskid":  t.taskId,
		"code":    code,
		"message": message,
		"data":    data,
	}
	return t.nc.Exec("task.error", par)
}
