package main

import (
	"github.com/jaracil/nxcli/demos/go/sugar"
	nexus "github.com/jaracil/nxcli/nxcore"
)

func main() {
	// Service
	s, ok := sugar.NewServiceFromConfig("handler")
	if !ok {
		return
	}

	// A handler for all methods
	s.SetHandler(func(task *nexus.Task) (interface{}, *nexus.JsonRpcErr) {
		if task.Method == "hello" {
			return "bye", nil
		}
		return nil, &nexus.JsonRpcErr{nexus.ErrMethodNotFound, "", nil}
	})

	// Serve
	s.Serve()
}
