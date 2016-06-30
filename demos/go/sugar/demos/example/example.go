package main

import (
	"time"

	"github.com/jaracil/ei"
	"github.com/jaracil/nxcli/demos/go/sugar/config"
	. "github.com/jaracil/nxcli/demos/go/sugar/log"
	nexus "github.com/jaracil/nxcli/nxcore"
)

func main() {
	// Service
	s, err := config.NewService()
	if err != nil {
		Log.Errorln(err.Error())
		return
	}

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
		Log.Errorln(err.Error())
	}
}
