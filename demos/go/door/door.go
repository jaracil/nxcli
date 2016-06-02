package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"time"

	nexus "github.com/jaracil/nxcli/nxcore"
	"github.com/jessevdk/go-flags"
)

var opts struct {
	Host    string `long:"host" description:"Nexus tcp://[user:pass@]host[:port]" default:"test:test@localhost:1717/test.fibonacci"`
	Threads int    `long:"threads" description:"Simultaneous threads" default:"5"`
}

func openDoor(task *nexus.Task) bool {
	conn, err := net.Dial("tcp", "192.168.1.30:5010")
	if err != nil {
		task.SendError(1, "No encuentro la puerta", nil)
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second * 5))
	_, err = conn.Write([]byte("\xa5\x00\x00\x00\x01^\x00\x00\xbc\x19"))
	if err != nil {
		task.SendError(2, "Error al enviar la trama a la puerta", nil)
		return false
	}
	res := make([]byte, 1024)
	_, err = conn.Read(res)
	if err != nil {
		task.SendError(3, "La puerta no responde", nil)
		return false

	}
	if !bytes.Contains(res, []byte("\xa5\x00\x00\x00\x01\xde\x00\x00\x00\x90R")) {
		task.SendError(4, "La puerta responde cosas indescifrables", nil)
		return false
	}
	return true
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
		if task.Method == "open" {
			if openDoor(task) {
				task.SendResult("Puerta abierta!!")
			}
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
		log.Fatalln("Not user:pass")
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
