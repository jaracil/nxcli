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
	Prefix string `long:"prefix" description:"Nexus listen prefix" default:"test.sugar.example"`
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
		Pulls:       5,         // Number of concurrent task pulls
		PullTimeout: time.Hour, // Timeout for task pulls
		MaxThreads:  50,        // Maximum number of threads running concurrently
	})
	s.SetDebugEnabled(true)                       // Output debug info
	s.SetDebugStatsPeriod(time.Millisecond * 100) // Output stats periodically if debug is enabled

	// A method that returns the service available methods
	s.AddMethod("methods", func(task *nexus.Task) {
		task.SendResult(s.GetMethods())
	})

	// A method that panics: a nexus.ErrInternal error will be returned as a result
	s.AddMethod("panic", func(task *nexus.Task) {
		panic("¿What if a method panics?")
		task.SendResult("ok")
	})

	// A method that calls Stop()
	s.AddMethod("exit", func(task *nexus.Task) {
		task.SendResult("why?")
		s.Stop()
	})

	// A method that calls GracefulStop()
	s.SetGracefulExitTime(time.Second * 10) // Wait nexus connection and workers for 10 seconds
	s.AddMethod("gracefulExit", func(task *nexus.Task) {
		task.SendResult("meh!")
		s.GracefulStop()
	})

	// A fibonacci method
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
	err = s.Serve()
	if err != nil {
		log.Println(err.Error())
	}
}
