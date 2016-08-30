package nexus

import (
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

func Version() string {
	return _version.String()
}

func (v *version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}