package nexus

import (
	"strconv"
	"strings"
	"fmt"
)

var _version = &version{
	Major: 0,
	Minor: 2,
	Patch: 0,
}

type version struct {
	Major int
	Minor int
	Patch int
}

var Version = _version.String()

func isVersionCompatible(v string) bool {
	if v == "" {
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

func (v *version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
