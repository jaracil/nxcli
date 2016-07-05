package main

import (
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

	// A handler for all methods
	s.SetHandler(func(task *nexus.Task) (interface{}, *nexus.JsonRpcErr) {
		if task.Method == "hello" {
			return "bye", nil
		}
		return nil, &nexus.JsonRpcErr{nexus.ErrMethodNotFound, "", nil}
	})

	// Serve
	err = s.Serve()
	if err != nil {
		Log.Errorln(err.Error())
	}
}
