package main

import (
	"log"
	"time"

	"github.com/jaracil/nxcli/demos/go/sugar"
	nexus "github.com/jaracil/nxcli/nxcore"
)

func main() {
	s := sugar.NewService("tcp://test:test@nexus.n4m.zone:1717", "test.sugar.handler", &sugar.ServiceOpts{
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

	err := s.Serve()
	if err != nil {
		log.Println(err.Error())
	}
}
