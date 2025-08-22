package ui

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"

	"github.com/go-playground/validator"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

var errNoConfigFile = fmt.Errorf("no config file loaded")

type Config struct {
	// where to look for log files? example: "./*.log"
	FilesGlobPattern string `validate:"required" yaml:"files_glob_pattern"`
	// where to store the index and other data (relative to cwd supported)
	StoragePath string `validate:"path_exists" yaml:"storage_path"`
	// a regular expression to find the start of messages in a heap file,
	// it must contain the date pattern in the first matching group
	// example: "^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\]"
	MessageStartRE string `validate:"required,regexp" yaml:"message_start_re"`
	// the pattern of a date in a message
	// see https://go.dev/src/time/format.go
	DateFormat string `validate:"required" yaml:"date_format"`
	// sets the degree of concurrency in the service (affects ingestion and search),
	// defaults to the number of cores if omitted or <1.
	Concurrency int `yaml:"concurrency"`
	// Terms are extracted from messages and indexed.
	// These control how fast ingestion goes (and space taken for the inverted index),
	// as well as how fast search goes (as shorter terms may duplicate in the index).
	MinTermLen int `yaml:"min_term_len"`
	MaxTermLen int `yaml:"max_term_len"`
	// Max memory the duckdb instance is allowed to allocate.
	// Increase if you see related errors on big data sets. (default: 500)
	DuckdbMaxMemMb int `yaml:"duckdb_max_mem_mb"`
}

// Validate is the final check after all overrides are done (file load, command arguments substituted)
func (cfg Config) Validate() error {
	// Validate config values
	translateError := func(e validator.FieldError) string {
		switch e.ActualTag() {
		case "path_exists":
			return fmt.Sprintf("path \"%v\" does not exist", e.Value())
		case "required":
			return "value is empty"
		case "regexp":
			return "invalid regular expression"
		default:
			return fmt.Sprintf("invalid value (%s)", e.Tag())
		}
	}

	cfgValidate := validator.New()

	err := cfgValidate.RegisterValidation(
		"path_exists", func(fl validator.FieldLevel) bool {
			path := fl.Field().String()
			if !filepath.IsAbs(path) {
				cwd, _ := os.Getwd()
				path = filepath.Join(cwd, path)
			}
			_, err := os.Stat(path)
			return err == nil
		},
	)
	if err != nil {
		return err
	}

	err = cfgValidate.RegisterValidation(
		"regexp", func(fl validator.FieldLevel) bool {
			_, err := regexp.Compile(fl.Field().String())
			return err == nil
		},
	)
	if err != nil {
		return err
	}

	err = cfgValidate.Struct(cfg)
	if err != nil {
		message := "Invalid config values:\n"
		for _, err := range err.(validator.ValidationErrors) {
			message += fmt.Sprintf("> %v: %s\n", err.StructField(), translateError(err))
		}
		return errors.New(message)
	}

	if cfg.MinTermLen > cfg.MaxTermLen {
		return errors.New("min term length cannot be greater than max term length")
	}

	return nil
}

var DefaultCfg = Config{
	StoragePath:      "./",
	FilesGlobPattern: "./*.log",
	MinTermLen:       4,
	MaxTermLen:       8,
	DuckdbMaxMemMb:   500,
	Concurrency:      runtime.NumCPU(),
}

func LoadConfig() (cfg Config, err error) {

	cfg = DefaultCfg

	viper.AddConfigPath(".")
	viper.SetConfigName("heaplog")

	err = viper.ReadInConfig()
	if err == nil {
		err = viper.Unmarshal(
			&cfg, func(dc *mapstructure.DecoderConfig) {
				dc.TagName = "yaml"
			},
		)
		if err != nil {
			err = fmt.Errorf("unable to decode into config struct: %w", err)
		}
	} else {
		// Check config read errors
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			err = errNoConfigFile
			return
		} else {
			err = fmt.Errorf("unable to use config file: %s", err)
			return
		}
	}

	if cfg.Concurrency < 1 {
		cfg.Concurrency = DefaultCfg.Concurrency
	}

	return cfg, cfg.Validate()
}
