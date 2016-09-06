package nexus

import (
	"strconv"
	"strings"

	"github.com/jaracil/ei"
	"github.com/jaracil/nxcli/nxcore"
)

var Version = &nxcore.NxVersion{
	Major: 1,
	Minor: 0,
	Patch: 0,
}

func isVersionCompatible(v *nxcore.NxVersion) bool {
	if v == nil || v.String() == "0.0.0" {
		return false
	}
	if v.Major != Version.Major {
		return false
	}
	return true
}

func getNexusVersion(nc *nxcore.NexusConn) *nxcore.NxVersion {
	res, err := nc.Exec("sys.version", nil)
	if err == nil {
		return parseVersionString(ei.N(res).M("version").StringZ())
	}
	return parseVersionString("0.0.0")
}

func parseVersionString(v string) *nxcore.NxVersion {
	if verspl := strings.Split(v, "."); len(verspl) == 3 {
		if major, err := strconv.Atoi(verspl[0]); err == nil {
			if minor, err := strconv.Atoi(verspl[1]); err == nil {
				if patch, err := strconv.Atoi(verspl[2]); err == nil {
					return &nxcore.NxVersion{major, minor, patch}
				}
			}
		}
	}
	return &nxcore.NxVersion{0, 0, 0}
}
