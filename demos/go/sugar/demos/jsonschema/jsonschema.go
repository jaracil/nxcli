package main

import (
	"time"

	"github.com/jaracil/nxcli/demos/go/sugar/config"
	. "github.com/jaracil/nxcli/demos/go/sugar/log"
	nexus "github.com/jaracil/nxcli/nxcore"
)

var MyOpts struct {
	Sleep float64 `short:"z" description:"Sleep before sending result" default:"0"`
}

func main() {
	// Config
	config.AddFlags("myopts", &MyOpts)
	err := config.Parse()
	if err != nil {
		Log.Errorln(err.Error())
		return
	}
	if MyOpts.Sleep < 0 {
		MyOpts.Sleep = 0
	}
	Log.Infof("My opts: %+v", MyOpts)

	// Service
	s, err := config.NewService()
	if err != nil {
		Log.Errorln(err.Error())
		return
	}
	s.AddMethodSchema("person", `{"title":"Person","type":"object","properties":{"name":{"type":"string","description":"First and Last name","minLength":4,"default":"Jeremy Dorn"},"age":{"type":"integer","default":25,"minimum":18,"maximum":99},"gender":{"type":"string","enum":["male","female"]},"location":{"type":"object","title":"Location","properties":{"city":{"type":"string","default":"San Francisco"},"state":{"type":"string","default":"CA"},"citystate":{"type":"string","description":"This is generated automatically from the previous two fields","template":"{{city}}, {{state}}","watch":{"city":"location.city","state":"location.state"}}}}}}`,
		func(task *nexus.Task) {
			time.Sleep(time.Duration(float64(time.Second) * MyOpts.Sleep))
			task.SendResult(task.Params)
		},
	)

	// Serve
	err = s.Serve()
	if err != nil {
		Log.Errorln(err.Error())
	}
}
