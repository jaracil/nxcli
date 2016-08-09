package nxcore

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jaracil/ei"
)

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
		Id:     t.M("taskid").StringZ(),
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

// TaskList returns how many push/pulls are happening on a path and its children
// Returns a TaskList or error.
func (nc *NexusConn) TaskList(prefix string, limit int, skip int) ([]Task, error) {
	par := map[string]interface{}{
		"prefix": prefix,
		"limit":  limit,
		"skip":   skip,
	}
	res, err := nc.Exec("task.list", par)
	if err != nil {
		return nil, err
	}

	list := make([]Task, 0)
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &list)
	if err != nil {
		return nil, err
	}

	return list, nil
}

// SendResult closes Task with result.
// Returns the response object from Nexus or error.
func (t *Task) SendResult(res interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.Id,
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
		"taskid":  t.Id,
		"code":    code,
		"message": message,
		"data":    data,
	}
	return t.nc.Exec("task.error", par)
}

// Reject rejects the task. Task is returned to Nexus tasks queue.
func (t *Task) Reject() (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.Id,
	}
	return t.nc.Exec("task.reject", par)
}

// Accept accepts a detached task. Is an alias for SendResult(nil).
func (t *Task) Accept() (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.Id,
		"result": nil,
	}
	return t.nc.Exec("task.result", par)
}

// GetConn retrieves the task underlying nexus connection.
func (t *Task) GetConn() *NexusConn {
	return t.nc
}
