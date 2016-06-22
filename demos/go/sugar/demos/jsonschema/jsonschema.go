package main

import (
	"log"
	"time"

	"github.com/jaracil/nxcli/demos/go/sugar"
	nexus "github.com/jaracil/nxcli/nxcore"
)

var MyOpts struct {
	Sleep float64 `short:"z" description:"Sleep before sending result" default:"0"`
}

func main() {
	// Config
	sugar.Config.AddFlags("myopts", &MyOpts)
	err := sugar.Config.Parse()
	if err != nil {
		log.Println(err.Error())
		return
	}
	if MyOpts.Sleep < 0 {
		MyOpts.Sleep = 0
	}
	log.Printf("My opts: %+v\n", MyOpts)

	// Service
	s, err := sugar.NewServiceFromConfig()
	if err != nil {
		log.Println(err.Error())
		return
	}
	s.AddMethodSchema("person", `{"title":"Person","type":"object","properties":{"name":{"type":"string","description":"First and Last name","minLength":4,"default":"Jeremy Dorn"},"age":{"type":"integer","default":25,"minimum":18,"maximum":99},"gender":{"type":"string","enum":["male","female"]},"location":{"type":"object","title":"Location","properties":{"city":{"type":"string","default":"San Francisco"},"state":{"type":"string","default":"CA"},"citystate":{"type":"string","description":"This is generated automatically from the previous two fields","template":"{{city}}, {{state}}","watch":{"city":"location.city","state":"location.state"}}}}}}`,
		func(task *nexus.Task) {
			time.Sleep(time.Second * time.Duration(MyOpts.Sleep))
			task.SendResult(task.Params)
		},
	)
	log.Printf("My service: %s\n", s)

	// Serve
	err = s.Serve()
	if err != nil {
		log.Println(err.Error())
	}
}
