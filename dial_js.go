// +build js

package nexus

import (
	"net/url"

	"github.com/goxjs/websocket"
)

func Dial(s string, opts *DialOptions) (*NexusConn, error) {

	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	conn, err := websocket.Dial(u.String(), opts.WsConfig.Origin)

	if err != nil {
		return nil, err
	}

	return NewNexusConn(conn), nil
}
