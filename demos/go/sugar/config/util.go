package config

import (
	"bufio"
	"io"
	"regexp"
	"strings"
)

func parseIniFile(inifile io.Reader) map[string]map[string]string {
	scanner := bufio.NewScanner(inifile)
	result := make(map[string]map[string]string)
	section := ""

	reSection := regexp.MustCompile(`^\[(.*)\]$`)
	reKeyValue := regexp.MustCompile(`^([^=]+?)\s*=\s*(.*)$`)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 || line[0] == ';' || line[0] == '#' {
			continue
		}
		if m := reSection.FindStringSubmatch(line); m != nil {
			section = strings.ToLower(m[1])
			continue
		}
		if m := reKeyValue.FindStringSubmatch(line); m != nil {
			key := strings.ToLower(m[1])
			value := m[2]
			if _, ok := result[section]; !ok {
				result[section] = map[string]string{}
			}
			result[section][key] = strings.Trim(value, "\"'")
			continue
		}
	}
	return result
}
