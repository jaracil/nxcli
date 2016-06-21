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

	// A method with a schema
	s.AddMethodSchema("person", `{"title":"Person","type":"object","properties":{"name":{"type":"string","description":"First and Last name","minLength":4,"default":"Jeremy Dorn"},"age":{"type":"integer","default":25,"minimum":18,"maximum":99},"gender":{"type":"string","enum":["male","female"]},"location":{"type":"object","title":"Location","properties":{"city":{"type":"string","default":"San Francisco"},"state":{"type":"string","default":"CA"},"citystate":{"type":"string","description":"This is generated automatically from the previous two fields","template":"{{city}}, {{state}}","watch":{"city":"location.city","state":"location.state"}}}}}}`,
		func(task *nexus.Task) {
			task.SendResult(task.Params)
		},
	)

	// Serve
	err = s.Serve()
	if err != nil {
		log.Println(err.Error())
	}
}
