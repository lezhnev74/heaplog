package ui

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
	"log"
)

func overrideConfig(cfg Config, ctx *cli.Context) Config {
	if ctx.String("FilesGlobPattern") != "" {
		cfg.FilesGlobPattern = ctx.String("FilesGlobPattern")
	}
	if ctx.String("StoragePath") != "" {
		cfg.StoragePath = ctx.String("StoragePath")
	}
	if ctx.String("MessageStartRE") != "" {
		cfg.MessageStartRE = ctx.String("MessageStartRE")
	}
	if ctx.String("DateFormat") != "" {
		cfg.DateFormat = ctx.String("DateFormat")
	}
	if ctx.Int("Concurrency") != 0 {
		cfg.Concurrency = uint(ctx.Int("Concurrency"))
	}
	if ctx.Int("MinTermLen") != 0 {
		cfg.MinTermLen = uint(ctx.Int("MinTermLen"))
	}
	if ctx.Int("MaxTermLen") != 0 {
		cfg.MaxTermLen = uint(ctx.Int("MaxTermLen"))
	}
	if ctx.Int("DuckdbMaxMemMb") != 0 {
		cfg.DuckdbMaxMemMb = uint(ctx.Int("DuckdbMaxMemMb"))
	}

	return cfg
}

func PrepareConsoleApp() (app *cli.App) {

	prepareCfg := func(ctx *cli.Context) (Config, error) {
		cfg, _ := LoadConfig(true)
		cfg = overrideConfig(cfg, ctx)
		return cfg, cfg.Validate()
	}

	flags := []cli.Flag{
		&cli.StringFlag{
			Name:    "FilesGlobPattern",
			Aliases: []string{"files"},
			Usage:   "where to look for log files? example: \"./*.log\"",
		},
		&cli.StringFlag{
			Name:    "StoragePath",
			Aliases: []string{"storage"},
			Usage:   "where to store the index and other data (relative to cwd supported)",
		},
		&cli.StringFlag{
			Name:    "MessageStartRE",
			Aliases: []string{"pattern", "re"},
			Usage:   "a regular expression to find the start of messages in a heap file, it must contain the date pattern in the first matching group",
		},
		&cli.StringFlag{
			Name:    "DateFormat",
			Aliases: []string{"date"},
			Usage:   "the pattern of a date in a message (go-format, see https://go.dev/src/time/format.go)",
		},
		&cli.IntFlag{
			Name:    "Concurrency",
			Aliases: []string{"c"},
			Usage:   "sets the degree of concurrency in the service (affects ingestion and search)",
		},
		&cli.IntFlag{
			Name:    "MinTermLen",
			Aliases: []string{"minterm"},
			Usage:   "min indexed term length",
		},
		&cli.IntFlag{
			Name:    "MaxTermLen",
			Aliases: []string{"maxterm"},
			Usage:   "max indexed term length",
		},
		&cli.IntFlag{
			Name:    "DuckdbMaxMemMb",
			Aliases: []string{"duckdb"},
			Usage:   "Max memory the duckdb instance is allowed to allocate (Mb)",
		},
	}

	app = &cli.App{
		Name: "heaplog",
		Commands: []*cli.Command{
			{
				Name:        "run",
				Description: "Runs the service (indexes files and exposes search UI over HTTP)",
				Flags:       flags,
				Action: func(ctx *cli.Context) error {
					cfg, err := prepareCfg(ctx)
					if err != nil {
						return err
					}
					happ, err := NewHeaplog(cfg, true)
					if err != nil {
						return err
					}

					httpApp := makeHttpApp(happ, "")
					log.Printf("Listening on port 8393")
					log.Fatal(httpApp.Listen(":8393"))
					return nil
				},
			},
			{
				Name:        "test",
				Flags:       flags,
				Description: "Tests config and tries to extract a single message from one of the log files",
				Action: func(ctx *cli.Context) error {
					cfg, err := prepareCfg(ctx)
					if err != nil {
						return err
					}
					happ, err := NewHeaplog(cfg, false)
					if err != nil {
						return err
					}
					return happ.Test()
				},
			},
			{
				Name:        "gen",
				Flags:       flags,
				Description: "Generates config to stdOut.",
				Action: func(ctx *cli.Context) error {
					cfg := overrideConfig(DefaultCfg, ctx)
					cfg.MessageStartRE = "<PUT YOUR REGULAR EXPRESSION>"
					cfg.DateFormat = "<PUT YOUR DATE FORMAT>"
					err := cfg.Validate()
					if err != nil {
						return err
					}
					yamlData, err := yaml.Marshal(&cfg)
					if err != nil {
						return err
					}
					fmt.Print(string(yamlData))
					return nil
				},
			},
		},
	}

	return
}
