package nxcore

import "encoding/json"

// UserCreate creates new user in Nexus user's table.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserCreate(user, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	return nc.Exec("user.create", par)
}

// UserDelete removes user from Nexus user's table.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserDelete(user string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
	}
	return nc.Exec("user.delete", par)
}

type UserInfo struct {
	User string                            `json:"user"`
	Tags map[string]map[string]interface{} `json:"tags"`
}

// UserList lists users from Nexus user's table.
// Returns a list of UserInfo or error.
func (nc *NexusConn) UserList(prefix string, limit int, skip int) ([]UserInfo, error) {
	par := map[string]interface{}{
		"prefix": prefix,
		"limit":  limit,
		"skip":   skip,
	}
	res, err := nc.Exec("user.list", par)
	if err != nil {
		return nil, err
	}
	users := make([]UserInfo, 0)
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &users)
	if err != nil {
		return nil, err
	}

	return users, nil
}

// UserSetTags set tags on user's prefix.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserSetTags(user string, prefix string, tags map[string]interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"user":   user,
		"prefix": prefix,
		"tags":   tags,
	}
	return nc.Exec("user.setTags", par)
}

// UserDelTags remove tags from user's prefix.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserDelTags(user string, prefix string, tags []string) (interface{}, error) {
	par := map[string]interface{}{
		"user":   user,
		"prefix": prefix,
		"tags":   tags,
	}
	return nc.Exec("user.delTags", par)
}

// UserSetPass sets new user password.
// Returns the response object from Nexus or error.
func (nc *NexusConn) UserSetPass(user string, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	return nc.Exec("user.setPass", par)
}
