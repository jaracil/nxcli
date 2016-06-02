package main

import (
	"fmt"

	"github.com/gopherjs/gopherjs/js"
	"github.com/jaracil/nxcli"
	"github.com/jaracil/nxcli/nxcore"
)

func main() {

	// Node.js
	if require := js.Global.Get("require"); require != js.Undefined {
		if ws := require.Invoke("ws"); ws != js.Undefined {
			fmt.Println("Running on Node.js with websocket module: 'ws'")

			if removeListener := ws.Get("prototype").Get("removeEventListener"); removeListener == js.Undefined {
				fmt.Println("Missing removeEventListener! Patching WebSocket library...")

				ws.Get("prototype").Set("removeEventListener", js.MakeFunc(func(this *js.Object, args []*js.Object) interface{} {
					this.Call("removeListener", args[0], args[1])
					return nil
				}))

				fmt.Println("OK")
			}

			js.Global.Set("WebSocket", ws)
		} else {
			fmt.Println("Running on Node.js but websocket module missing")
		}
	}

	dial := func(a string, cb func(interface{}, error)) {
		go func() {
			nc, e := nexus.Dial(a, nil)

			jsnc := js.MakeWrapper(nc)
			PatchNexusAsync(jsnc, nc)

			go cb(jsnc, e)
		}()
	}

	if js.Module == js.Undefined {
		// Browser
		js.Global.Set("Nexus", make(map[interface{}]interface{}))
		nexus := js.Global.Get("Nexus")
		nexus.Set("Dial", dial)
		nexus.Set("Errors", nxcore.ErrStr)
	} else {
		// Node.js
		js.Module.Get("exports").Set("dial", dial)
	}

	fmt.Println("Nexus Client loaded")
}
