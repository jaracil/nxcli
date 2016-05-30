Nexus Javascript / Node.js Client
=================================

Nexus client for web browser/node.js built using gopherjs, wrapping around the golang nexus client

## Requirements
  * [GopherJS](https://github.com/gopherjs/gopherjs)
    * ```go get github.com/gopherjs/gopherjs```
  
### Requirements for node.js:
  * WebSocket module ['ws'](https://github.com/websockets/ws)
    * ```npm -g install ws```
    
    
# Examples

## Pull a task from a browser
```javascript
dial("wss://localhost.n4m.zone", function(nc, err){

  nc.Login("dummyUser", "dummyPassword", function(){
  
    nc.TaskPull("test.prefix", 5, function(task, err){
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
  
    nc.PipeCreate({"len": 100}, function(pipe, e){
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

  
