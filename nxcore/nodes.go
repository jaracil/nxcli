package nxcore

import "encoding/json"

type NodeInfo struct {
	Load    map[string]float64 `json:"load"`
	Clients int                `json:"clients"`
	NodeId  string             `json:"id"`
	Version string             `json:"version"`
}

// Nodes returns info of the nodes state
// Returns a list of NodeInfo structs or an error
func (nc *NexusConn) NodeList(limit int, skip int) ([]NodeInfo, error) {
	par := map[string]interface{}{
		"limit": limit,
		"skip":  skip,
	}
	res, err := nc.Exec("sys.node.list", par)
	if err != nil {
		return nil, err
	}
	nodes := make([]NodeInfo, 0)
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &nodes)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}
