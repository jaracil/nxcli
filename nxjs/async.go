package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jaracil/nxcli/nxcore"

	"github.com/gopherjs/gopherjs/js"
)

func ret(r interface{}, e error, cb []*js.Object) {
	switch len(cb) {
	case 1:
		cb[0].Invoke(r, e)

	case 2:
		if e == nil {
			cb[0].Invoke(r)
		} else {
			cb[1].Invoke(e)
		}
	}
}

func ret3(r1 interface{}, r2 interface{}, e error, cb []*js.Object) {
	switch len(cb) {
	case 1:
		cb[0].Invoke(r1, r2, e)

	case 2:
		if e == nil {
			cb[0].Invoke(r1, r2)
		} else {
			cb[1].Invoke(e)
		}
	}
}

func PatchTaskAsync(jstask *js.Object, task *nxcore.Task) {

	jstask.Set("SendResult", func(res interface{}, cb ...*js.Object) {
		go func() {
			r, e := task.SendResult(res)
			ret(r, e, cb)
		}()
	})

	jstask.Set("SendError", func(code int, msg string, data interface{}, cb ...*js.Object) {
		go func() {
			r, e := task.SendError(code, msg, data)
			ret(r, e, cb)
		}()
	})

	jstask.Set("Path", task.Path)
	jstask.Set("Method", task.Method)
	jstask.Set("Params", task.Params)
	jstask.Set("Tags", task.Tags)

}

func PatchPipeAsync(jspipe *js.Object, pipe *nxcore.Pipe) {
	jspipe.Set("Close", func(cb ...*js.Object) {
		go func() {
			r, e := pipe.Close()
			ret(r, e, cb)
		}()
	})

	jspipe.Set("Read", func(max int, timeout int, cb ...*js.Object) {
		go func() {
			r, e := pipe.Read(max, time.Duration(timeout)*time.Second)
			if e != nil {
				ret(nil, e, cb)
				return
			}

			jd, _ := json.Marshal(r)
			pd := make(map[string]interface{})
			json.Unmarshal(jd, &pd)

			ret(pd, e, cb)
		}()
	})

	jspipe.Set("Write", func(msg interface{}, cb ...*js.Object) {
		go func() {
			r, e := pipe.Write(msg)
			ret(r, e, cb)
		}()
	})

	jspipe.Set("Id", func(cb ...*js.Object) string {
		go func() {
			r := pipe.Id()
			fmt.Println("ID", r)
			ret(r, nil, cb)
		}()

		return pipe.Id()
	})
}

func PatchNexusAsync(jsnc *js.Object, nc *nxcore.NexusConn) {

	jsnc.Set("Login", func(user string, pass string, cb ...*js.Object) {
		go func() {
			r, e := nc.Login(user, pass)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("TaskPush", func(method string, params interface{}, timeout int, cb ...*js.Object) {
		go func() {
			r, e := nc.TaskPush(method, params, time.Duration(timeout)*time.Second)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("TaskPull", func(prefix string, timeout int, cb ...*js.Object) {
		go func() {
			r, e := nc.TaskPull(prefix, time.Duration(timeout)*time.Second)
			if e != nil {
				ret(nil, e, cb)
				return
			}

			jstask := js.MakeWrapper(r)
			PatchTaskAsync(jstask, r)

			ret(jstask, e, cb)
		}()
	})

	jsnc.Set("UserCreate", func(user string, pass string, cb ...*js.Object) {
		go func() {
			r, e := nc.UserCreate(user, pass)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("UserDelete", func(user string, cb ...*js.Object) {
		go func() {
			r, e := nc.UserDelete(user)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("UserDelTags", func(user string, prefix string, tags []string, cb ...*js.Object) {
		go func() {
			r, e := nc.UserDelTags(user, prefix, tags)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("UserSetPass", func(user string, pass string, cb ...*js.Object) {
		go func() {
			r, e := nc.UserSetPass(user, pass)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("UserSetTags", func(user string, prefix string, tags map[string]interface{}, cb ...*js.Object) {
		go func() {
			r, e := nc.UserSetTags(user, prefix, tags)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("PipeCreate", func(jopts interface{}, cb ...*js.Object) {
		go func() {
			d, e := json.Marshal(jopts)
			if e != nil {
				ret(nil, e, cb)
				return
			}
			var opts *nxcore.PipeOpts
			json.Unmarshal(d, &opts)

			r, e := nc.PipeCreate(opts)
			jspipe := js.MakeWrapper(r)

			PatchPipeAsync(jspipe, r)

			ret(jspipe, e, cb)
		}()
	})

	jsnc.Set("PipeOpen", func(id string, cb ...*js.Object) {
		go func() {
			r, e := nc.PipeOpen(id)
			if e != nil {
				ret(nil, e, cb)
				return
			}

			jspipe := js.MakeWrapper(r)
			PatchPipeAsync(jspipe, r)
			ret(jspipe, e, cb)
		}()
	})

	jsnc.Set("ChanPublish", func(channel string, msg interface{}, cb ...*js.Object) {
		go func() {
			r, e := nc.ChanPublish(channel, msg)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("ChanSubscribe", func(pipe *nxcore.Pipe, channel string, cb ...*js.Object) {
		go func() {
			r, e := nc.ChanSubscribe(pipe, channel)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("ChanUnsubscribe", func(pipe *nxcore.Pipe, channel string, cb ...*js.Object) {
		go func() {
			r, e := nc.ChanUnsubscribe(pipe, channel)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("Exec", func(method string, params interface{}, cb ...*js.Object) {
		go func() {
			r, e := nc.Exec(method, params)
			ret(r, e, cb)
		}()
	})

	jsnc.Set("ExecNoWait", func(method string, params interface{}, cb ...*js.Object) {
		go func() {
			r1, r2, e := nc.ExecNoWait(method, params)

			d, e := json.Marshal(r2)
			if e != nil {
				ret(nil, e, cb)
				return
			}
			var res *nxcore.JsonRpcRes
			json.Unmarshal(d, &res)

			ret3(r1, res, e, cb)
		}()
	})

	jsnc.Set("Cancel", func(cb ...*js.Object) {
		go func() {
			nc.Cancel()
			ret(nil, nil, cb)
		}()
	})

	jsnc.Set("Closed", func(cb ...*js.Object) {
		go func() {
			r := nc.Closed()
			ret(r, nil, cb)
		}()
	})

	jsnc.Set("Ping", func(timeout int, cb ...*js.Object) {
		go func() {
			e := nc.Ping(time.Duration(timeout) * time.Second)
			ret(nil, e, cb)
		}()
	})
}
