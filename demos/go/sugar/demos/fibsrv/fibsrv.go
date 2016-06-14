package main

import (
	"log"
	"time"

	"github.com/jaracil/ei"
	"github.com/jaracil/nxcli/demos/go/sugar"
	nexus "github.com/jaracil/nxcli/nxcore"
	"github.com/jessevdk/go-flags"
)

var opts struct {
	Host   string `long:"host" description:"Nexus tcp://[user:pass@]host[:port]" default:"tcp://test:test@localhost:1717"`
	Prefix string `long:"prefix" description:"Nexus listen prefix" default:"test.sugar.fibsrv"`
}

func main() {
	// Parse options
	_, err := flags.Parse(&opts)
	if err != nil {
		log.Println(err.Error())
		return
	}

	// Define service
	s := sugar.NewService(opts.Host, opts.Prefix, &sugar.ServiceOpts{
		Pulls:       5,
		PullTimeout: time.Hour,
		MaxThreads:  10,
	})

	// Set methods
	s.AddMethod("fib", func(task *nexus.Task) {
		// Parse params
		v, err := ei.N(task.Params).M("v").Int()
		if err != nil {
			task.SendError(nexus.ErrInvalidParams, "", nil)
			return
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
		task.SendResult(r)
	})

	// Serve
	log.Fatalln(s.Serve())
}
