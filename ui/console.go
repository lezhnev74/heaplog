package ui

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"heaplog_2024/common"
	"heaplog_2024/scanner"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"

	go_iterators "github.com/lezhnev74/go-iterators"
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
	cfg.EnableLogging = ctx.Bool("Verbose")

	return cfg
}

func PrepareConsoleApp() (app *cli.App) {

	prepareCfg := func(ctx *cli.Context) (Config, error) {
		cfg, _ := LoadConfig(true)
		cfg = overrideConfig(cfg, ctx)

		common.EnableLogging = cfg.EnableLogging

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
			Name:    "Verbose",
			Aliases: []string{"v"},
			Usage:   "Show extra details about what the service is doing",
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

					log.Printf("Pid: %d", os.Getpid())

					cfg, err := prepareCfg(ctx)
					if err != nil {
						return err
					}

					_ctx, cancel := context.WithCancel(context.Background())
					happ, err := NewHeaplog(_ctx, cfg, true)
					if err != nil {
						cancel()
						return err
					}
					httpApp := makeHttpApp(happ, "")

					sigs := make(chan os.Signal, 1)
					signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
					go func() {
						<-sigs
						cancel() // stop the program
						log.Printf("Stopping heaplog...")

						t := time.Second * 3
						time.Sleep(t)

						err := httpApp.Shutdown()
						if err != nil {
							common.Out("%s", err)
						}
					}()

					go func() {
						log.Println("Listening pprof on port 6060")
						log.Println(http.ListenAndServe(":6060", nil))
					}()

					log.Printf("Listening on port 8393")
					log.Fatal(httpApp.Listen(":8393"))
					return nil
				},
			},
			{
				Name:        "run_readonly",
				Description: "Runs search only (no indexing or any other writing is happening)",
				Flags:       flags,
				Action: func(ctx *cli.Context) error {
					cfg, err := prepareCfg(ctx)
					if err != nil {
						return err
					}
					happ, err := NewHeaplog(context.Background(), cfg, false)
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
				Name:        "query",
				Description: "Send a single query",
				Flags: append(flags, &cli.StringFlag{
					Name:    "QueryText",
					Aliases: []string{"query", "q"},
					Usage:   "A normal query as a string",
				}),
				Action: func(ctx *cli.Context) error {
					cfg, err := prepareCfg(ctx)
					if err != nil {
						return err
					}
					happ, err := NewHeaplog(context.Background(), cfg, false)
					if err != nil {
						return err
					}

					outStream := os.Stdout

					// 1. Make a query
					queryText := ctx.String("QueryText")
					if len(queryText) == 0 {
						return fmt.Errorf("empty query")
					}
					q, _, err := happ.NewQuery(queryText, nil, nil)
					if err != nil {
						return err
					}

					// 2. Read all the data into a destination stream
					rows, err := happ.All(q.Id, nil, nil)
					if err != nil {
						return err
					}

					for {
						messageString, err := rows.Next()
						if errors.Is(err, go_iterators.EmptyIterator) {
							break
						} else if err != nil {
							return err
						}

						_, err = outStream.Write([]byte(messageString + "\n"))
						if err != nil {
							return err
						}
					}

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
					happ, err := NewHeaplog(context.Background(), cfg, false)
					if err != nil {
						return err
					}
					return happ.Test()
				},
			},
			{
				Name:        "detect",
				Flags:       flags,
				Description: "Detect the correct regexp pattern for the dates in your log files",
				Action: func(ctx *cli.Context) error {

					fmt.Print("Enter a sample message line:\n")
					reader := bufio.NewReader(os.Stdin)
					input, err := reader.ReadString('\n')
					if err != nil {
						return err
					}
					// try to find the date
					startPattern, format, err := scanner.DetectMessageLine([]byte(input))
					if err != nil {
						return fmt.Errorf("Detection failed: %s\n", err)
					}

					pattern := scanner.TimeFormatToRegexp(format)
					r := regexp.MustCompile(pattern)
					matches := r.FindStringSubmatch(input)
					if len(matches) != 1 {
						return fmt.Errorf("Detection failed\n")
					}

					datePos := strings.Index(input, matches[0])
					fmt.Printf("%s%s\n", strings.Repeat(" ", datePos), strings.Repeat("^", len(matches[0])))
					fmt.Printf("%sYay, the date detected above!\n\n", strings.Repeat(" ", datePos))
					fmt.Printf("Config values:\n")
					fmt.Printf("MessageStartRE: '%s'\nDateFormat: '%s'\n", startPattern, format)

					return nil
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
