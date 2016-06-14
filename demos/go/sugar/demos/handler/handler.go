package main

import (
	"log"
	"time"

	"github.com/jaracil/nxcli/demos/go/sugar"
	nexus "github.com/jaracil/nxcli/nxcore"
	"github.com/jessevdk/go-flags"
)

var opts struct {
	Host   string `long:"host" description:"Nexus tcp://[user:pass@]host[:port]" default:"tcp://test:test@localhost:1717"`
	Prefix string `long:"prefix" description:"Nexus listen prefix" default:"test.sugar.handler"`
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
		MaxThreads:  50,
	})

	s.SetHandler(func(task *nexus.Task) {
		if task.Method == "hello" {
			task.SendResult("bye")
			return
		}
		task.SendError(nexus.ErrInvalidParams, "", nil)
	})

	err = s.Serve()
	if err != nil {
		log.Println(err.Error())
	}
}
