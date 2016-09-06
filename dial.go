// +build !js

package nexus

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/jaracil/nxcli/nxcore"
	"golang.org/x/net/websocket"
)

var ErrVersionIncompatible = fmt.Errorf("incompatible version")

type DialOptions struct {
	WsConfig  *websocket.Config
	TlsConfig *tls.Config
}

func NewDialOptions() *DialOptions {
	conf, _ := websocket.NewConfig("http://localhost", "http://nexusclient.go")
	conf.TlsConfig = &tls.Config{}
	return &DialOptions{
		WsConfig:  conf,
		TlsConfig: &tls.Config{},
	}
}

func Dial(s string, opts *DialOptions) (*nxcore.NexusConn, error) {

	var conn net.Conn
	var err error

	// If host doesnt have a schema, prepend tcp://
	if ok, _ := regexp.MatchString("[a-zA-Z0-9]*://.*", s); !ok {
		s = "tcp://" + s
	}

	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = NewDialOptions()
	}

	// If there is a host:port, use it
	// If no port has been specified, search for SRV records
	// If no SRV records are found, use the host and try with default ports

	type H struct {
		Host string
		Port int
	}
	var hosts []H

	if sHost := strings.Split(u.Host, ":"); len(sHost) == 1 {
		if _, addrs, err := net.LookupSRV("nexus", u.Scheme, u.Host); err == nil && len(addrs) > 0 {
			for _, addr := range addrs {
				hosts = append(hosts, H{Host: strings.TrimSuffix(addr.Target, "."), Port: int(addr.Port)})
			}
		} else {
			hosts = append(hosts, H{Host: u.Host})
		}
	} else {
		port, _ := strconv.Atoi(sHost[1])
		hosts = append(hosts, H{Host: sHost[0], Port: port})
	}

	///
	/// Try to connect to every host found in the previous step
	///

	for _, v := range hosts {
		switch u.Scheme {
		default:
			fallthrough
		case "tcp":
			if v.Port == 0 {
				v.Port = 1717
			}
			t := fmt.Sprintf("%s:%d", v.Host, v.Port)
			conn, err = net.Dial("tcp", t)

		case "ssl":
			if v.Port == 0 {
				v.Port = 1718
			}
			t := fmt.Sprintf("%s:%d", v.Host, v.Port)
			conn, err = tls.Dial("tcp", t, opts.TlsConfig)

		case "ws":
			if v.Port == 0 {
				v.Port = 80
			}
			fallthrough

		case "wss":
			if v.Port == 0 {
				v.Port = 443
			}
			t := fmt.Sprintf("%s:%d", v.Host, v.Port)

			if opts.WsConfig.TlsConfig == nil {
				opts.WsConfig.TlsConfig = opts.TlsConfig
			}
			opts.WsConfig.Location, err = url.Parse(fmt.Sprintf("%s://%s%s", u.Scheme, t, u.Path))
			if err != nil {
				fmt.Println("Error parsing ws location:", err)
				continue
			}
			conn, err = websocket.DialConfig(opts.WsConfig)
		}

		if err != nil {
			continue
		}
	}

	if len(hosts) == 0 || err != nil {
		return nil, err
	}

	nxconn := nxcore.NewNexusConn(conn)

	nxconn.NexusVersion = getNexusVersion(nxconn)
	if !isVersionCompatible(nxconn.NexusVersion) {
		return nxconn, ErrVersionIncompatible
	}

	return nxconn, nil
}
