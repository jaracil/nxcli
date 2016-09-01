package nxcore

import (
	"encoding/json"
	"time"
)

type UserSessions struct {
	User     string        `json:"user"`
	Sessions []SessionInfo `json:"sessions"`
	N        int           `json:"n"`
}

type SessionInfo struct {
	Id            string    `json:"connid"`
	NodeId        string    `json:"nodeid"`
	RemoteAddress string    `json:"remoteAddress"`
	Protocol      string    `json:"protocol"`
	CreationTime  time.Time `json:"creationTime"`
}

// Sessions returns info of the users sessions
// Returns a list of SessionInfo structs or an error
func (nc *NexusConn) SessionList(prefix string, limit int, skip int) ([]UserSessions, error) {
	par := map[string]interface{}{
		"prefix": prefix,
		"limit":  limit,
		"skip":   skip,
	}
	res, err := nc.Exec("sys.session.list", par)
	if err != nil {
		return nil, err
	}
	sessions := make([]UserSessions, 0)
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &sessions)
	if err != nil {
		return nil, err
	}

	return sessions, nil
}

// SessionKick forces the node owner of the client connection to close it
// Returns the response object from Nexus or error.
func (nc *NexusConn) SessionKick(connId string) (interface{}, error) {
	par := map[string]interface{}{
		"connid": connId,
	}
	return nc.Exec("sys.session.kick", par)
}

// SessionReload forces the node owner of the client connection to reload its info (tags)
// Returns the response object from Nexus or error.
func (nc *NexusConn) SessionReload(connId string) (interface{}, error) {
	par := map[string]interface{}{
		"connid": connId,
	}
	return nc.Exec("sys.session.reload", par)
}
