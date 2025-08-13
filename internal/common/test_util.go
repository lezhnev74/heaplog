package common

import (
	"os"
)

const MessageStartPattern = `(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`
const TimeFormat = "2006-01-02T15:04:05.000000-07:00"

func PopulateFiles(c map[string][]byte) error {
	for p, content := range c {
		err := os.WriteFile(p, content, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}
