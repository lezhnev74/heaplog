package ui

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search/query_language"
)

func overrideConfig(cfg Config, cmd *cli.Command) Config {
	if cmd.String("FilesGlobPattern") != "" {
		cfg.FilesGlobPattern = cmd.String("FilesGlobPattern")
	}
	if cmd.String("StoragePath") != "" {
		cfg.StoragePath = cmd.String("StoragePath")
	}
	if cmd.String("MessageStartRE") != "" {
		cfg.MessageStartRE = cmd.String("MessageStartRE")
	}
	if cmd.String("DateFormat") != "" {
		cfg.DateFormat = cmd.String("DateFormat")
	}
	if cmd.Int("Concurrency") != 0 {
		cfg.Concurrency = cmd.Int("Concurrency")
	}
	if cmd.Int("MinTermLen") != 0 {
		cfg.MinTermLen = cmd.Int("MinTermLen")
	}
	if cmd.Int("MaxTermLen") != 0 {
		cfg.MaxTermLen = cmd.Int("MaxTermLen")
	}
	if cmd.Int("DuckdbMaxMemMb") != 0 {
		cfg.DuckdbMaxMemMb = cmd.Int("DuckdbMaxMemMb")
	}

	return cfg
}
func NewConsole(c context.Context, logger *zap.Logger, frontendPublic fs.FS) *cli.Command {
	prepareCfg := func(cmd *cli.Command) (Config, error) {
		cfg, err := LoadConfig()
		if err != nil {
			return cfg, err
		}
		cfg = overrideConfig(cfg, cmd)
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
		&cli.BoolFlag{
			Name:    "Profile",
			Aliases: []string{"profile"},
			Usage:   "Dump mem profile to heaplog.pprof",
		},
	}

	cmd := &cli.Command{
		Commands: []*cli.Command{
			{
				Name:        "run",
				Flags:       flags,
				Description: "Run indexing and start a web UI",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Bool("Profile") {
						stop, err := common.Profile()
						if err != nil {
							return err
						}
						defer stop()
					}

					cfg, err := LoadConfig()
					if err != nil && errors.Is(err, errNoConfigFile) {
						logger.Info("No config file found, using default config")
					} else if err != nil {
						return err
					}

					heaplog := NewHeaplog(c, logger, cfg)

					// INGESTION
					ingestionInFlight := false
					common.RepeatEvery(
						ctx, 60*time.Second, func() {
							if ingestionInFlight {
								return
							}
							ingestionInFlight = true
							defer func() { ingestionInFlight = false }()

							err := heaplog.Ingestor.Run()
							if err != nil {
								heaplog.Logger.Error("Ingestor failed", zap.Error(err))
							}
						},
					)

					// II MERGING
					mergingInFlight := false
					common.RepeatEvery(
						ctx, 60*time.Second, func() {
							if mergingInFlight {
								return
							}
							mergingInFlight = true
							defer func() { mergingInFlight = false }()

							for {
								merged, err := heaplog.II.Merge(20, 40, cfg.Concurrency)
								if err != nil {
									heaplog.Logger.Error("II merging failed", zap.Error(err))
								}
								if merged == 0 {
									break
								}
								heaplog.Logger.Info(fmt.Sprintf("Merged %d segments in II", merged))
							}
						},
					)

					httpApp := NewHttpApp(c, http.FS(frontendPublic), heaplog)
					go func() {
						<-ctx.Done()
						_ = httpApp.ShutdownWithTimeout(3 * time.Second)
					}()
					return httpApp.Listen(":3000")
				},
			},
			{
				Name:        "search",
				Flags:       flags,
				Description: "Search via the console",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Bool("Profile") {
						stop, err := common.Profile()
						if err != nil {
							return err
						}
						defer stop()
					}

					cfg, err := LoadConfig()
					if err != nil && errors.Is(err, errNoConfigFile) {
						logger.Info("No config file found, using default config")
					} else if err != nil {
						return err
					}

					query := cmd.Args().First()
					expr, err := query_language.ParseUserQuery(query)
					if err != nil {
						return fmt.Errorf("failed to parse query: %w", err)
					}

					heaplog := NewHeaplog(c, logger, cfg)
					msgs, err := heaplog.Searcher.Search(expr, nil, nil)
					if err != nil {
						return err
					}

					for m := range msgs {
						fmt.Println(string(m.Body))
					}

					return nil
				},
			},
			{
				Name:        "gen",
				Flags:       flags,
				Description: "Generates config to stdOut.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					cfg := overrideConfig(DefaultCfg, cmd)
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
			{
				Name:        "test",
				Flags:       flags,
				Description: "Tests config and tries to extract a single message from one of the log files",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					cfg, err := prepareCfg(cmd)
					if err != nil {
						return err
					}
					file, err := TestConfig(cfg)
					if err != nil {
						return err
					}
					fmt.Printf("Great! Found a message in %s\n", file)
					return nil
				},
			},
		},
	}

	return cmd
}
