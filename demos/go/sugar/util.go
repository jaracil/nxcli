package sugar

func inStrSlice(elem string, elems []string) bool {
	for _, el := range elems {
		if el == elem {
			return true
		}
	}
	return false
}
