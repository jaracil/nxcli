package main

import (
	"log"

	"github.com/jaracil/nxcli/demos/go/sugar"
	nexus "github.com/jaracil/nxcli/nxcore"
)

func main() {
	// Service
	s, err := sugar.NewServiceFromConfig()
	if err != nil {
		log.Println(err.Error())
		return
	}

	// A handler for all methods
	s.SetHandler(func(task *nexus.Task) {
		if task.Method == "hello" {
			task.SendResult("bye")
			return
		}
		task.SendError(nexus.ErrInvalidParams, "", nil)
	})

	// Serve
	err = s.Serve()
	if err != nil {
		log.Println(err.Error())
	}
}
