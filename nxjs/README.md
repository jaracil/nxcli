Nexus Javascript / Node.js Client
=================================

Nexus client for web browser/node.js built using gopherjs, wrapping around the golang nexus client

## Requirements
  * [GopherJS](https://github.com/gopherjs/gopherjs)
    * ```go get github.com/gopherjs/gopherjs```
  
### Requirements for node.js:
  * WebSocket module ['ws'](https://github.com/websockets/ws)
    * ```npm -g install ws```
    

# API
The API is the same than the golang client, ([TODO: Link when available](#)) but functions have one or two optional parameters at the end for callbacks.

If there is only one callback parameter, it should be a function with two arguments:
```javascript
	TaskPull("prefix", 60, function(result, error) {
		console.log("This is the result:", result)
		console.log("Error received:", error)
	}
```

With two callback parameters, one will receive the result and the other the error:
```javascript
	Login("user", "pass", function(result) {
		console.log("Logged in!")
	}, function(error) {
		console.log("Couldn't login!:", error)
	})
```


# Examples

## Pull a task from a browser
```javascript
// The module will set dial as a global function when loaded
dial("wss://localhost.n4m.zone", function(nc, err){

	// Login to nexus
  nc.Login("dummyUser", "dummyPassword", function(){
  
  	// Success! Now pull a task
    nc.TaskPull("test.prefix", 5, function(task, err){
    
    	// Great! Just return an OK
      console.log(task, err);
      task.SendResult("OK");
    })
    
  })
})
```

## Subscribe a pipe to a channel

```javascript
var nexus = require("./nxjs.js")

nexus.dial("wss://localhost.n4m.zone", function(nc, err){
  nc.Login("dummyUser", "dummyPass", function(){
  
  	// Create a pipe
    nc.PipeCreate({"len": 100}, function(pipe, e){
    
    	// Subscribe the pipe to the channel
      nc.ChanSubscribe(pipe, "temperatures",
      
        //Subscription succeeded
        function(){
          console.log("Subscribed pipe", pipe.Id(), "to channel temperatures")
        
          pipe.Read(10, 60, function(msgs, err){
           console.log("Received messages:", msgs)
          })
        },
        
        // Subscription failed
        function(err){ console.log("Error subscribing the pipe to the channel:", err)})
    })
  })
})
```

  
