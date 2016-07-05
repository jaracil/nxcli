package main

import (
	"time"

	"github.com/jaracil/ei"
	"github.com/jaracil/nxcli/demos/go/sugar"
	. "github.com/jaracil/nxcli/demos/go/sugar/log"
	nexus "github.com/jaracil/nxcli/nxcore"
)

func main() {
	// Service
	s := sugar.NewService("test:test@localhost", "test.sugar.fibsrv", &sugar.ServiceOpts{4, time.Hour, 12})
	s.SetLogLevel("debug")
	s.SetStatsPeriod(time.Second * 5)

	// A method that computes fibonacci
	s.AddMethod("fib", func(task *nexus.Task) (interface{}, *nexus.JsonRpcErr) {
		// Parse params
		v, err := ei.N(task.Params).M("v").Int()
		if err != nil {
			return nil, &nexus.JsonRpcErr{nexus.ErrInvalidParams, "", nil}
		}
		tout := ei.N(task.Params).M("t").Int64Z()

		// Do work
		if tout > 0 {
			time.Sleep(time.Duration(tout) * time.Second)
		}
		r := []int{}
		for i, j := 0, 1; j < v; i, j = i+j, i {
			r = append(r, i)
		}
		return r, nil
	})

	// Serve
	err := s.Serve()
	if err != nil {
		Log.Errorln(err.Error())
	}
}
