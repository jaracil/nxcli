// +build js

package nexus

import (
	"net/url"

	"github.com/goxjs/websocket"
	"github.com/jaracil/nxcli/nxcore"
)

func Dial(s string, _ interface{}) (*nxcore.NexusConn, error) {

	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	conn, err := websocket.Dial(u.String(), "http://gopherjs.nexus")

	if err != nil {
		return nil, err
	}

	return nxcore.NewNexusConn(conn), nil
}
