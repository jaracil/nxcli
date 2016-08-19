package main

import (
	"time"

	"github.com/jaracil/ei"
	"github.com/jaracil/nxcli/demos/go/sugar"
	. "github.com/jaracil/nxcli/demos/go/sugar/log"
	nexus "github.com/jaracil/nxcli/nxcore"
)

func main() {
	// Service
	s, err := sugar.NewServiceFromConfig("jsonschema")
	if err != nil {
		return
	}

	// Config
	config, err := sugar.GetConfig()
	if err != nil {
		return
	}

	sleep := ei.N(config).M("services").M("jsonschema").M("sleep").IntZ()

	if sleep < 0 {
		sleep = 0
	}

	s.Log(InfoLevel, "My sleep opt: %d", sleep)

	s.AddMethodSchema("person", `{"title":"Person","type":"object","properties":{"name":{"type":"string","description":"First and Last name","minLength":4,"default":"Jeremy Dorn"},"age":{"type":"integer","default":25,"minimum":18,"maximum":99},"gender":{"type":"string","enum":["male","female"]},"location":{"type":"object","title":"Location","properties":{"city":{"type":"string","default":"San Francisco"},"state":{"type":"string","default":"CA"},"citystate":{"type":"string","description":"This is generated automatically from the previous two fields","template":"{{city}}, {{state}}","watch":{"city":"location.city","state":"location.state"}}}}}}`,
		func(task *nexus.Task) (interface{}, *nexus.JsonRpcErr) {
			time.Sleep(time.Second * time.Duration(sleep))
			return task.Params, nil
		},
	)

	// Serve
	s.Serve()
}
