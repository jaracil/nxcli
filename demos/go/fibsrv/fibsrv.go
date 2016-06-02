package main

import (
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/jaracil/ei"
	nexus "github.com/jaracil/nxcli/nxcore"
	"github.com/jessevdk/go-flags"
)

var opts struct {
	Host    string `long:"host" description:"Nexus tcp://[user:pass@]host[:port]" default:"test:test@localhost:1717/test.fibonacci"`
	Threads int    `long:"threads" description:"Simultaneous threads" default:"5"`
}

func fib(n int) (r []int) {
	for i, j := 0, 1; j < n; i, j = i+j, i {
		r = append(r, i)
	}
	return
}

func listenner(nc *nexus.NexusConn, prefix string) {
	for {
		task, err := nc.TaskPull(prefix, time.Hour)
		if err != nil {
			if err.(*nexus.JsonRpcErr).Code() == nexus.ErrTimeout {
				continue
			}
			log.Fatalln("Error pulling task", err.Error())
		}
		fmt.Printf("Atendiendo petición:%+v\n", task)
		if task.Method == "fib" {
			v, err := ei.N(task.Params).M("v").Int()
			if err != nil {
				task.SendError(nexus.ErrInvalidParams, "", nil)
				continue
			}
			tout := ei.N(task.Params).M("t").Int64Z()
			if tout > 0 {
				time.Sleep(time.Duration(tout) * time.Second)
			}
			task.SendResult(fib(v))
		} else {
			task.SendError(nexus.ErrMethodNotFound, "", nil)
			continue
		}
	}
}

func main() {
	_, err := flags.Parse(&opts)
	if err != nil {
		os.Exit(1)
	}
	u, err := url.Parse(opts.Host)
	if err != nil {
		log.Fatalln("Can't parse nexus host")
	}

	conn, err := net.Dial(u.Scheme, u.Host)
	if err != nil {
		log.Fatalln("Can't connect to nexus server")
	}
	nc := nexus.NewNexusConn(conn)
	if u.User == nil {
		log.Fatalln("user/pass not found")
	}
	pass, _ := u.User.Password()
	prefix := u.Path[1:]
	_, err = nc.Login(u.User.Username(), pass)
	if err != nil {
		log.Fatalln("Can't login")
	}
	for x := 0; x < opts.Threads; x++ {
		go listenner(nc, prefix)
	}
	for {
		time.Sleep(time.Second)
	}
}
