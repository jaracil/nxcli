package nxcore

import "github.com/jaracil/ei"

// Lock tries to get a lock.
// Returns lock success/failure or error.
func (nc *NexusConn) Lock(lock string) (interface{}, error) {
	par := ei.M{
		"lock": lock,
	}
	res, err := nc.Exec("sync.lock", par)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Unlock tries to free a lock.
// Returns unlock success/failure or error.
func (nc *NexusConn) Unlock(lock string) (interface{}, error) {
	par := ei.M{
		"lock": lock,
	}
	res, err := nc.Exec("sync.unlock", par)
	if err != nil {
		return nil, err
	}
	return res, nil
}
