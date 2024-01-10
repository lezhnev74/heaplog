package heaplog

import (
	"fmt"
	"github.com/araddon/dateparse"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// DetectMessageLine accepts a line with a known date and extracts the settings
// return a regex pattern for the line beginning that includes date in the 1st matching group
func DetectMessageLine(text []byte) (startPattern string, dateFormat string, err error) {
	nonDateChars := `[^\d\w\.,\s:\(\)/+-]`
	p := regexp.MustCompile(nonDateChars)
	for i := 0; i < len(text); i++ {
		m := p.FindIndex(text[i+1:])
		if m == nil {
			break
		}
		subline := text[i : m[0]+i+1]
		msgDate, err := dateparse.ParseAny(string(subline))
		if err != nil {
			continue
		}
		dateFormat, err = dateparse.ParseFormat(string(subline))
		if err != nil {
			continue
		}
		// use the found date To detect prefix
		dateRestored := msgDate.Format(dateFormat)
		pos := strings.Index(string(text), dateRestored)
		if pos == -1 {
			continue
		}
		prefix := string(text[:pos])
		// put the date To the 1st matching group
		escapedPrefix := ""
		for _, r := range prefix {
			if strings.ContainsRune(`.,+*?^$()[]{}|\-`, r) {
				escapedPrefix += `\`
			}
			escapedPrefix += string(r)
		}
		startPattern = fmt.Sprintf("(?m)^%s(%s)", escapedPrefix, TimeFormatToRegexp(dateFormat))
		return startPattern, dateFormat, nil
	}
	return "", "", fmt.Errorf("unable To detect messages")
}

// TimeFormatToRegexp returns a regexp pattern that can regognize
// any date in the given Time Format
func TimeFormatToRegexp(format string) (pattern string) {
	type unit struct {
		unitPattern string
		unitCount   int
	}
	groups := make([]unit, 0)
	addUnit := func(unitPattern string) {
		if len(groups) == 0 || groups[len(groups)-1].unitPattern != unitPattern {
			groups = append(groups, unit{unitPattern, 0})
		}
		groups[len(groups)-1].unitCount++
	}

	prefixMap := [][]string{
		{"__2", `\d{1,3}`},
		{"_2", `\d{1,2}`},
		// Numeric time zone offsets
		{"-070000", `[+-]\d{6}`},
		{"-0700", `[+-]\d{4}`},
		{"-07", `[+-]\d{2}`},
		// timezone
		{"Z070000", `(?:\w)|(?:[+-]\d{6})`},
		{"Z0700", `(?:\w)|(?:[+-]\d{4})`},
		{"Z07", `(?:\w)|(?:[+-]\d{2})`},
	}

	i := 0
mainloop:
	for i < len(format) {
		for _, prefix := range prefixMap {
			if strings.HasPrefix(format[i:], prefix[0]) {
				addUnit(prefix[1])
				i += len(prefix[0])
				continue mainloop
			}
		}

		r, rSize := utf8.DecodeRuneInString(format[i:])
		switch true {
		case unicode.IsSpace(r):
			addUnit(`\s+`)
		case unicode.IsDigit(r):
			addUnit(`\d`)
		case unicode.IsLetter(r):
			addUnit(`\w`)
		case strings.ContainsRune(`.,+*?^$()[]{}|\-`, r):
			addUnit(`\` + string(r))
		default:
			addUnit(string(r))
		}
		i += rSize
	}

	for _, unitGroup := range groups {
		pattern += unitGroup.unitPattern
		if unitGroup.unitCount > 1 {
			pattern += fmt.Sprintf(`{%d}`, unitGroup.unitCount)
		}
	}
	return
}
