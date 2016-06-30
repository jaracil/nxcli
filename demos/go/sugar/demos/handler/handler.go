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
		Log.Errorln(err.Error())
	}
}
