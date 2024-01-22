package ui

import (
	"fmt"
	"github.com/go-playground/validator"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

type Config struct {
	// where to look for log files? example: "./*.log"
	FilesGlobPattern string `validate:"required" yaml:"FilesGlobPattern"`
	// where to store the index and other data (relative to cwd supported)
	StoragePath string `validate:"required,path_exists" yaml:"StoragePath"`
	// a re pattern to find the start of a message in a heap file
	// it can contain the date pattern in the first matching group
	// see https://github.com/google/re2/wiki/Syntax
	// example: (?m)^(...date pattern...)
	MessageStartRE string `validate:"required,regexp" yaml:"MessageStartRE"`
	// the pattern of a date in a message
	// see https://go.dev/src/time/format.go
	DateFormat string `validate:"required" yaml:"DateFormat"`
	// sets parallel degree of ingesting.
	// defaults to the number of cores is omitted or 0.
	IngestWorkers uint
	// Terms are extracted from messages and indexed.
	// These control how fast ingestion goes (and space taken for the inverted index),
	// as well as how fast search goes (as shorter terms may duplicate in the index).
	MinTermLen, MaxTermLen uint
	// Max memory the duckdb instance is allowed to allocate.
	// Increase if you see related errors on big data sets. (default: 500)
	DuckdbMaxMemMb uint
}

var DefaultCfg = Config{
	StoragePath:      "./",
	FilesGlobPattern: "./*.log",
}

func LoadConfig(loadFile bool) (cfg Config) {
	var err error

	cfg = DefaultCfg

	// Load config
	viper.AddConfigPath(".")
	viper.SetConfigName("heaplog")
	if loadFile == true {
		err = viper.ReadInConfig()
		if err == nil {
			err = viper.Unmarshal(&cfg)
			if err != nil {
				log.Fatalf("unable to decode into config struct, %s", err)
			}
		} else {
			// Check config read errors
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				log.Printf("no config file loaded")
			} else {
				log.Fatalf("unable to use config file: %s", err)
			}
		}
	}

	// Override with flags
	if viper.GetString("StoragePath") != "" {
		cfg.StoragePath = viper.GetString("StoragePath")
	}
	if viper.GetString("FilesGlobPattern") != "" {
		cfg.FilesGlobPattern = viper.GetString("FilesGlobPattern")
	}
	if viper.GetString("MessageStartRE") != "" {
		cfg.MessageStartRE = viper.GetString("MessageStartRE")
	}
	if viper.GetString("DateFormat") != "" {
		cfg.DateFormat = viper.GetString("DateFormat")
	}

	// Validate config values
	translateError := func(e validator.FieldError) string {
		switch e.ActualTag() {
		case "path_exists":
			return fmt.Sprintf("path \"%v\" does not exist", e.Value())
		case "required":
			return "value is empty"
		case "regexp":
			return "invalid regular expression, see https://github.com/google/re2/wiki/Syntax"
		default:
			return fmt.Sprintf("invalid value (%s)", e.Tag())
		}
	}

	cfgValidate := validator.New()
	err = cfgValidate.RegisterValidation("path_exists", func(fl validator.FieldLevel) bool {
		path := fl.Field().String()
		if !filepath.IsAbs(path) {
			cwd, _ := os.Getwd()
			path = filepath.Join(cwd, path)
		}
		_, err := os.Stat(path)
		return err == nil
	})
	err = cfgValidate.RegisterValidation("regexp", func(fl validator.FieldLevel) bool {
		_, err := regexp.Compile(fl.Field().String())
		return err == nil
	})
	if err != nil {
		log.Fatal(err)
	}
	err = cfgValidate.Struct(cfg)
	if err != nil {
		message := "Invalid config values:\n"
		for _, err := range err.(validator.ValidationErrors) {
			message += fmt.Sprintf("> %v: %s\n", err.StructField(), translateError(err))
		}
		log.Fatalf(message)
	}

	return
}
