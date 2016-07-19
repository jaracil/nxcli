package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jaracil/ei"
	nxcli "github.com/jaracil/nxcli"
	nexus "github.com/jaracil/nxcli/nxcore"
	"github.com/nayarsystems/kingpin"
)

var (
	app      = kingpin.New("cli", "Nexus command line interface")
	serverIP = app.Flag("server", "Server address.").Default("127.0.0.1:1717").Short('s').String()
	timeout  = app.Flag("timeout", "Execution timeout").Default("60").Short('t').Int()
	user     = app.Flag("user", "Nexus username").Short('u').Default("test").String()
	pass     = app.Flag("pass", "Nexus password").Default("test").Short('p').String()

	///

	shell = app.Command("shell", "Interactive shell")

	///

	login     = app.Command("login", "Tests to login with an username/password and exits")
	loginName = login.Arg("username", "username").Required().String()
	loginPass = login.Arg("password", "password").Required().String()

	///

	push       = app.Command("push", "Execute a task.push rpc call on Nexus")
	pushMethod = push.Arg("method", "Method to call").Required().String()
	pushParams = push.Arg("params", "parameters").StringMap()

	pull       = app.Command("pull", "Execute a task.pull rpc call on Nexus")
	pullMethod = pull.Arg("prefix", "Method to call").Required().String()

	taskList       = app.Command("list", "Show push/pulls happening on a prefix")
	taskListPrefix = taskList.Arg("prefix", "prefix").Default("").String()
	taskListLimit  = taskList.Flag("limit", "Limit the number of tasks returned").Default("100").Int()
	taskListSkip   = taskList.Flag("skip", "Skip a number of tasks before applying the limit").Default("0").Int()

	///

	pipeCmd = app.Command("pipe", "Pipe tasks")

	pipeRead = pipeCmd.Command("read", "Create and read from a pipe. It will be destroyed on exit")

	pipeWrite     = pipeCmd.Command("open", "Open a pipe and send data")
	pipeWriteId   = pipeWrite.Arg("pipeId", "ID of the pipe to write to").Required().String()
	pipeWriteData = pipeWrite.Arg("data", "Data to write to the pipe").Required().Strings()

	///

	userCmd = app.Command("user", "user management")

	userCreate     = userCmd.Command("create", "Create a new user")
	userCreateName = userCreate.Arg("username", "username").Required().String()
	userCreatePass = userCreate.Arg("password", "password").Required().String()

	userDelete     = userCmd.Command("delete", "Delete an user")
	userDeleteName = userDelete.Arg("username", "username").Required().String()

	userPass     = userCmd.Command("passwd", "Change an user password")
	userPassName = userPass.Arg("username", "username").Required().String()
	userPassPass = userPass.Arg("password", "password").Required().String()

	userList       = userCmd.Command("list", "List users on a prefix")
	userListPrefix = userList.Arg("prefix", "prefix").Default("").String()
	userListLimit  = userList.Flag("limit", "Limit the number of users returned").Default("100").Int()
	userListSkip   = userList.Flag("skip", "Skip a number of elements before applying the limit").Default("0").Int()

	userKick       = userCmd.Command("kick", "Kick users on a prefix")
	userKickPrefix = userKick.Arg("prefix", "prefix").Required().String()
	
	userReload = userCmd.Command("reload", "Reloads users on a prefix")
	userReloadPrefix = userReload.Arg("prefix", "prefix").Required().String()

	///

	sessionsCmd = app.Command("sessions", "Show sessions info")

	sessionsList       = sessionsCmd.Command("list", "List active sessions")
	sessionsListPrefix = sessionsList.Arg("prefix", "User prefix").Default("").String()
	sessionsListLimit  = sessionsList.Flag("limit", "Limit the number of sessions returned").Default("100").Int()
	sessionsListSkip   = sessionsList.Flag("skip", "Skip a number of elements before applying the limit").Default("0").Int()

	sessionsKick     = sessionsCmd.Command("kick", "Kick any active connection with matching prefix")
	sessionsKickConn = sessionsKick.Arg("connId", "connId prefix").Required().String()
	
	sessionsReload = sessionsCmd.Command("reload", "Reload any active connection with matching prefix")
	sessionsReloadConn = sessionsReload.Arg("connId", "connId prefix").Required().String()

	///

	nodesCmd      = app.Command("nodes", "Show nodes info")
	nodesCmdLimit = nodesCmd.Flag("limit", "Limit the number of nodes returned").Default("100").Int()
	nodesCmdSkip  = nodesCmd.Flag("skip", "Skip a number of elements before applying the limit").Default("0").Int()

	///

	tagsCmd = app.Command("tags", "tags management")

	tagsSet       = tagsCmd.Command("set", "Set tags for an user on a prefix. Tags is a map like 'tag:value tag2:value2'")
	tagsSetUser   = tagsSet.Arg("user", "user").Required().String()
	tagsSetPrefix = tagsSet.Arg("prefix", "prefix").Required().String()
	tagsSetTags   = tagsSet.Arg("tags", "tag:value").StringMapIface()

	tagsSetJ         = tagsCmd.Command("setj", "Set tags for an user on a prefix. Tags is a json dict like: { 'tag': value }")
	tagsSetJUser     = tagsSetJ.Arg("user", "user").Required().String()
	tagsSetJPrefix   = tagsSetJ.Arg("prefix", "prefix").Required().String()
	tagsSetJTagsJson = tagsSetJ.Arg("tags", "{'@task.push': true}").Required().String()

	tagsDel       = tagsCmd.Command("del", "Delete tags for an user on a prefix. Tags is a list of space separated strings")
	tagsDelUser   = tagsDel.Arg("user", "user").Required().String()
	tagsDelPrefix = tagsDel.Arg("prefix", "prefix").Required().String()
	tagsDelTags   = tagsDel.Arg("tags", "tag1 tag2 tag3").Required().Strings()

	//

	chanCmd = app.Command("topic", "Topics management")

	chanSub     = chanCmd.Command("sub", "Subscribe a pipe to a topic")
	chanSubPipe = chanSub.Arg("pipe", "pipe id to subscribe").Required().String()
	chanSubChan = chanSub.Arg("topic", "Topic to subscribe to").Required().String()

	chanUnsub     = chanCmd.Command("unsub", "Unsubscribe a pipe from a topic")
	chanUnsubPipe = chanUnsub.Arg("pipe", "pipe id to subscribe").Required().String()
	chanUnsubChan = chanUnsub.Arg("topic", "Topic to subscribe to").Required().String()

	chanPub     = chanCmd.Command("pub", "Publish a message to a topic")
	chanPubChan = chanPub.Arg("topic", "Topic to subscribe to").Required().String()
	chanPubMsg  = chanPub.Arg("data", "Data to send").Required().Strings()
)

func main() {

	// Enable -h as HelpFlag
	app.HelpFlag.Short('h')
	//	/app.UsageTemplate(kingpin.CompactUsageTemplate)

	parsed := kingpin.MustParse(app.Parse(os.Args[1:]))

	if nc, err := nxcli.Dial(*serverIP, nil); err == nil {
		if res, err := nc.Login(*user, *pass); err != nil {
			log.Println("Couldn't login:", err)
			return
		} else {
			if ei.N(res).M("ok").BoolZ() {
				log.Println("Logged as", ei.N(res).M("user").StringZ())
			} else {
				log.Println("Unexpected reply:", res)
				return
			}
		}

		execCmd(nc, parsed)
	} else {
		log.Println("Cannot connect to", *serverIP)
	}
}

func execCmd(nc *nexus.NexusConn, parsed string) {
	switch parsed {

	case login.FullCommand():
		if _, err := nc.Login(*loginName, *loginPass); err != nil {
			log.Println("Couldn't login:", err)
			return
		} else {
			log.Println("Logged as", *loginName)
			user = loginName
		}

	case push.FullCommand():
		if ret, err := nc.TaskPush(*pushMethod, *pushParams, time.Second*time.Duration(*timeout)); err != nil {
			log.Println("Error:", err)
			return
		} else {
			b, _ := json.MarshalIndent(ret, "", "  ")
			log.Println("Result:")
			if s, err := strconv.Unquote(string(b)); err == nil {
				fmt.Println(s)
			} else {
				fmt.Println(string(b))
			}
		}

	case pull.FullCommand():
		log.Println("Pulling", *pullMethod)
		ret, err := nc.TaskPull(*pullMethod, time.Second*time.Duration(*timeout))
		if err != nil {
			log.Println("Error:", err)
			return
		} else {
			b, _ := json.MarshalIndent(ret, "", "  ")
			fmt.Println(string(b))
		}

		fmt.Printf("[R]esult or [E]rror? ")

		stdin := bufio.NewScanner(os.Stdin)

		if stdin.Scan() && strings.HasPrefix(strings.ToLower(stdin.Text()), "e") {
			fmt.Printf("Code: ")
			stdin.Scan()
			code, _ := strconv.Atoi(stdin.Text())

			fmt.Printf("Message: ")
			stdin.Scan()
			msg := stdin.Text()

			fmt.Printf("Data: ")
			stdin.Scan()
			data := stdin.Text()

			ret.SendError(code, msg, data)

		} else {
			fmt.Printf("Result: ")
			if stdin.Scan() {
				ret.SendResult(stdin.Text())
			} else {
				ret.SendResult("dummy response")
			}
		}

	case taskList.FullCommand():
		if res, err := nc.TaskList(*taskListPrefix, *taskListLimit, *taskListSkip); err != nil {
			log.Println(err)
			return
		} else {
			log.Printf("Pulls from [%s]:\n", *taskListPrefix)
			for path, n := range res.Pulls {
				log.Printf("\t[%s] - %d\n", path, n)
			}
			log.Printf("Pushes from [%s]:\n", *taskListPrefix)
			for path, n := range res.Pushes {
				log.Printf("\t[%s] - %d\n", path, n)
			}

		}

	case pipeWrite.FullCommand():
		// Clean afterwards in case we are looping on shell mode
		defer func() { *pipeWriteData = []string{} }()

		if pipe, err := nc.PipeOpen(*pipeWriteId); err != nil {
			log.Println(err)
			return
		} else {

			if _, err := pipe.Write(*pipeWriteData); err != nil {
				log.Println(err)
				return
			} else {
				log.Println("Sent!")
			}
		}

	case pipeRead.FullCommand():
		popts := nexus.PipeOpts{Length: 100}

		if pipe, err := nc.PipeCreate(&popts); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("Pipe created:", pipe.Id())
			for {
				if pdata, err := pipe.Read(10, time.Second*time.Duration(*timeout)); err != nil {
					log.Println(err)
					time.Sleep(time.Second)
				} else {
					for _, msg := range pdata.Msgs {
						log.Println("Got:", msg.Msg, msg.Count)
					}
					fmt.Printf("There are %d messages left in the pipe and %d drops\n", pdata.Waiting, pdata.Drops)
				}
			}
		}

	case userCreate.FullCommand():
		log.Printf("Creating user \"%s\" with password \"%s\"", *userCreateName, *userCreatePass)
		if _, err := nc.UserCreate(*userCreateName, *userCreatePass); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("OK")
		}

	case userDelete.FullCommand():
		log.Printf("Deleting user \"%s\"", *userDeleteName)

		if _, err := nc.UserDelete(*userDeleteName); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("OK")
		}

	case userList.FullCommand():
		log.Printf("Listing users on \"%s\"", *userListPrefix)

		if res, err := nc.UserList(*userListPrefix, *userListLimit, *userListSkip); err != nil {
			log.Println(err)
			return
		} else {
			for _, user := range res {
				log.Printf("User: [%s]\n", user.User)
				for prefix, tags := range user.Tags {
					log.Printf("\tPrefix: [%s]\n", prefix)
					for tag, val := range tags {
						log.Printf("\t\t%s: %v\n", tag, val)
					}
				}
			}
		}

	case userPass.FullCommand():
		if _, err := nc.UserSetPass(*userPassName, *userPassPass); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("OK")
		}

	case userKick.FullCommand():
		log.Printf("Kicking users on \"%s\"", *userKickPrefix)

		if res, err := nc.SessionList(*userKickPrefix, -1, -1); err != nil {
			log.Println(err)
			return
		} else {
			for _, session := range res {
				log.Printf("\tUser: [%s] - %d sessions", session.User, session.N)
				for _, ses := range session.Sessions {
					if kicked, err := nc.SessionKick(ses.Id); err == nil && ei.N(kicked).M("kicked").IntZ() == 1 {
						log.Printf("\t\tID: %s has been kicked", ses.Id)
					}
				}
			}
		}

	case userReload.FullCommand():
		log.Printf("Reloading users on \"%s\"", *userReloadPrefix)

		if res, err := nc.SessionList(*userReloadPrefix, -1, -1); err != nil {
			log.Println(err)
			return
		} else {
			for _, session := range res {
				log.Printf("\tUser: [%s] - %d sessions", session.User, session.N)
				for _, ses := range session.Sessions {
					if reloaded, err := nc.SessionReload(ses.Id); err == nil && ei.N(reloaded).M("reloaded").IntZ() == 1 {
						log.Printf("\t\tID: %s has been reloaded", ses.Id)
					}
				}
			}
		}

	case sessionsList.FullCommand():
		if res, err := nc.SessionList(*sessionsListPrefix, *sessionsListLimit, *sessionsListSkip); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("Sessions:")
			for _, session := range res {
				log.Printf("\tUser: [%s] - %d sessions", session.User, session.N)
				for _, ses := range session.Sessions {
					log.Printf("\t\tID: %s (Node:%s) - Protocol: %s - Remote: %s - Since: %s",
						ses.Id, ses.NodeId, ses.Protocol, ses.RemoteAddress, ses.CreationTime.Format("Mon Jan _2 15:04:05 2006"))
				}
			}
		}

	case sessionsKick.FullCommand():
		if res, err := nc.SessionKick(*sessionsKickConn); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("Sessions kicked:", ei.N(res).M("kicked").IntZ())
		}

	case sessionsReload.FullCommand():
		if res, err := nc.SessionReload(*sessionsReloadConn); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("Sessions reloaded:", ei.N(res).M("reloaded").IntZ())
		}

	case nodesCmd.FullCommand():
		if res, err := nc.NodeList(*nodesCmdLimit, *nodesCmdSkip); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("Nodes:")
			for _, node := range res {
				log.Printf("\tNodeId: [%s] - %d clients - Load: %0.2f/%0.2f/%0.2f", node.NodeId, node.Clients, node.Load["Load1"], node.Load["Load5"], node.Load["Load15"])

			}
		}

	case tagsSet.FullCommand():
		// Clean afterwards in case we are looping on shell mode
		defer func() { *tagsSetTags = make(map[string]interface{}) }()

		var tags map[string]interface{}
		if b, err := json.Marshal(*tagsSetTags); err == nil {
			if json.Unmarshal(b, &tags) != nil {
				log.Println("Error parsing tags")
				return
			}
		}

		log.Printf("Setting tags: %v on %s@%s", tags, *tagsSetUser, *tagsSetPrefix)
		if _, err := nc.UserSetTags(*tagsSetUser, *tagsSetPrefix, tags); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("OK")
		}

	case tagsSetJ.FullCommand():
		// Clean afterwards in case we are looping on shell mode

		var tags map[string]interface{}
		if json.Unmarshal([]byte(*tagsSetJTagsJson), &tags) != nil {
			log.Println("Error parsing tags json:", *tagsSetJTagsJson)
			return
		}

		log.Printf("Setting tags: %v on %s@%s", tags, *tagsSetJUser, *tagsSetJPrefix)
		if _, err := nc.UserSetTags(*tagsSetJUser, *tagsSetJPrefix, tags); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("OK")
		}

	case tagsDel.FullCommand():
		// Clean afterwards in case we are looping on shell mode
		defer func() { *tagsDelTags = []string{} }()

		if _, err := nc.UserDelTags(*tagsDelUser, *tagsDelPrefix, *tagsDelTags); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("OK")
		}

	case shell.FullCommand():

		args := os.Args[1:]
		for k, v := range args {
			if v == shell.FullCommand() {
				args = append(args[:k], args[k+1:]...)
			}
		}

		s := bufio.NewScanner(os.Stdin)
		fmt.Printf("%s@%s >> ", *user, *serverIP)
		for s.Scan() {
			cmd, err := app.Parse(append(args, strings.Split(s.Text(), " ")...))
			if err == nil {
				if cmd != shell.FullCommand() {
					parsed := kingpin.MustParse(cmd, err)
					execCmd(nc, parsed)
				}
			} else {
				log.Println(err)
			}
			fmt.Printf("%s@%s >> ", *user, *serverIP)
		}

		if err := s.Err(); err != nil {
			log.Fatalln("reading standard input:", err)
		}

	case chanSub.FullCommand():
		if pipe, err := nc.PipeOpen(*chanSubPipe); err != nil {
			log.Println(err)
			return
		} else {
			if _, err := nc.TopicSubscribe(pipe, *chanSubChan); err != nil {
				log.Println(err)
				return
			} else {
				log.Println("OK")
			}
		}

	case chanUnsub.FullCommand():
		if pipe, err := nc.PipeOpen(*chanSubPipe); err != nil {
			log.Println(err)
			return
		} else {
			if _, err := nc.TopicUnsubscribe(pipe, *chanUnsubChan); err != nil {
				log.Println(err)
				return
			} else {
				log.Println("OK")
			}
		}

	case chanPub.FullCommand():
		// Clean afterwards in case we are looping on shell mode
		defer func() { *chanPubMsg = []string{} }()

		if res, err := nc.TopicPublish(*chanPubChan, *chanPubMsg); err != nil {
			log.Println(err)
			return
		} else {
			log.Println("Result:", res)

		}
	}
}
