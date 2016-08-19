package main

import (
	"time"

	"github.com/jaracil/ei"
	"github.com/jaracil/nxcli/demos/go/sugar"
	nexus "github.com/jaracil/nxcli/nxcore"
)

func main() {
	// Server sets defaults for all services
	server, err := sugar.NewServerFromConfig()
	if err != nil {
		return
	}

	service1, err := server.AddService("service1")
	if err != nil {
		return
	}

	service2, err := server.AddService("service2")
	if err != nil {
		return
	}

	// A method that computes fibonacci on both services
	fib := func(task *nexus.Task) (interface{}, *nexus.JsonRpcErr) {
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
	}

	service1.AddMethod("fib", fib)
	service2.AddMethod("fib", fib)

	// Serve
	server.Serve()
}
