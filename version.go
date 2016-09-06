package nexus

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jaracil/ei"
	"github.com/jaracil/nxcli/nxcore"
)

var _version = &version{
	Major: 1,
	Minor: 0,
	Patch: 0,
}

type version struct {
	Major int
	Minor int
	Patch int
}

var Version = _version.String()

func isVersionCompatible(v string) bool {
	if v == "" || v == "0.0.0" {
		return false
	}
	if verspl := strings.Split(v, "."); len(verspl) != 0 {
		if major, err := strconv.Atoi(verspl[0]); err == nil {
			if major != _version.Major {
				return false
			}
		}
	}
	return true
}

func getNexusVersion(nc *nxcore.NexusConn) string {
	res, err := nc.Exec("sys.version", nil)
	if err == nil {
		return ei.N(res).M("version").StringZ()
	}
	return "0.0.0"
}

func (v *version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
