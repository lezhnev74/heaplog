package common

import (
	"os"
)

func PopulateFiles(c map[string][]byte) error {
	for p, content := range c {
		err := os.WriteFile(p, content, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}
