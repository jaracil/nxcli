package main

import (
	"github.com/gopherjs/gopherjs/js"
	"github.com/jaracil/nxcli"
	"github.com/jaracil/nxcli/nxcore"
)

func main() {
	// Node.js
	if require := js.Global.Get("require"); require != js.Undefined {
		if ws := require.Invoke("ws"); ws != js.Undefined {
			println("Running on Node.js with websocket module: 'ws'")

			ws.Get("prototype").Set("removeEventListener", js.MakeFunc(func(this *js.Object, args []*js.Object) interface{} {
				// Do nothing in nodejs, we can't remove EventListener
				return nil
			}))
			js.Global.Set("WebSocket", ws)
		} else {
			println("Running on Node.js but websocket module missing (try: npm install ws)")
		}
	}

	dial := func(a string, cb func(interface{}, error)) {
		go func() {
			nc, e := nexus.Dial(a, nil)
			cb(WrapNexusConn(nc), e)
		}()
	}

	var nexus *js.Object

	if js.Module == js.Undefined {
		// Browser
		js.Global.Set("nexus", make(map[interface{}]interface{}))
		nexus = js.Global.Get("nexus")
	} else {
		// Node.js
		nexus = js.Module.Get("exports")
	}

	nexus.Set("dial", dial)
	nexus.Set("errors", nxcore.ErrStr)

	println("Nexus Client loaded")
}
