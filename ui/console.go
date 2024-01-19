package ui

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	"heaplog/heaplog"
	"heaplog/scanner"
	"heaplog/tokenizer"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

func buildHeaplog(cfg Config) *heaplog.Heaplog {
	// controls the max length of tokens,
	// easier to index (consume less space), could be slower to search due to collisions.
	minTokenLen := 4
	if cfg.MinTermLen > 0 {
		minTokenLen = int(cfg.MinTermLen)
	}
	maxTokenLen := max(10, minTokenLen+1)
	if cfg.MaxTermLen > 0 {
		maxTokenLen = max(minTokenLen+1, int(cfg.MaxTermLen))
	}

	tokenizerFunc := func(input string) []string { return tokenizer.TokenizeS2(input, minTokenLen, maxTokenLen) }
	unboundTokenizerFunc := func(input string) []string { return tokenizer.TokenizeS2(input, 1, maxTokenLen) }

	ingestWorkers := int(cfg.IngestWorkers)
	if ingestWorkers == 0 {
		ingestWorkers = runtime.NumCPU()
		if ingestWorkers > 4 {
			// This is to reduce chances of concurrency for resources
			// a magick number :)
			ingestWorkers = ingestWorkers - 2
		}
	}

	hl, err := heaplog.NewHeaplog(
		cfg.StoragePath,
		regexp.MustCompile(cfg.MessageStartRE),
		cfg.DateFormat,
		strings.Split(cfg.FilesGlobPattern, ","),
		time.Millisecond*1_000,
		time.Millisecond*100,
		tokenizerFunc,
		unboundTokenizerFunc,
		50_000_000,
		ingestWorkers,
	)

	if err != nil {
		log.Fatal(err)
	}

	return hl
}

var rootCmd = &cobra.Command{
	Use:   "heaplog",
	Short: "Heaplog is a web search for local log files",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("choose one of the commands, use --help for more\n")
	},
}

var runCmd = &cobra.Command{
	Use:    "run",
	Short:  "Run heaplog daemon",
	PreRun: allowConfigFlags,
	Run: func(cmd *cobra.Command, args []string) {

		// // -- Start Tracing
		// traceFile, _ := os.Create("trace")
		// trace.Start(traceFile)
		// c := make(chan os.Signal, 1)
		// signal.Notify(c, os.Interrupt)
		// go func() {
		// 	<-c
		// 	trace.Stop()
		// 	traceFile.Close()
		// 	os.Exit(0)
		// }()
		// // -- End Tracing

		hl := buildHeaplog(LoadConfig(true))
		go hl.Background()

		cwd, _ := os.Getwd()
		viewsDirectory := filepath.Join(cwd, "./ui/web_templates") // todo replace with embed
		StartWebServer(hl, viewsDirectory)
	},
}

var initCmd = &cobra.Command{
	Use:    "init",
	Short:  "Produces empty config format",
	PreRun: allowConfigFlags,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := Config{
			FilesGlobPattern: "./*.log",
			StoragePath:      "./",
		}
		c, err := yaml.Marshal(cfg)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(string(c))
	},
}

var detectCmd = &cobra.Command{
	Use:   "detect",
	Short: "Help detect a correct message regexp pattern",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Print("Enter a sample message line:\n")
		reader := bufio.NewReader(os.Stdin)
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("An error occurred while reading input. Please try again", err)
			return
		}
		// try to find the date
		startPattern, format, err := heaplog.DetectMessageLine([]byte(input))
		if err != nil {
			log.Fatalf("Detection failed: %s\n", err)
		}

		pattern := heaplog.TimeFormatToRegexp(format)
		r := regexp.MustCompile(pattern)
		matches := r.FindStringSubmatch(input)
		if len(matches) != 1 {
			log.Fatalf("Detection failed\n")
			return
		}

		datePos := strings.Index(input, matches[0])
		fmt.Printf("%s%s\n", strings.Repeat(" ", datePos), strings.Repeat("^", len(matches[0])))
		fmt.Printf("%sYay, the date detected above!\n\n", strings.Repeat(" ", datePos))
		fmt.Printf("Config values:\n")
		fmt.Printf("MessageStartRE: \"%s\"\nDateFormat: \"%s\"\n", startPattern, format)
	},
}

var testCmd = &cobra.Command{
	Use:    "test",
	Short:  "Tests your config values",
	PreRun: allowConfigFlags,
	Args:   cobra.MatchAll(cobra.ExactArgs(1)),
	Run: func(cmd *cobra.Command, args []string) {
		cfg := LoadConfig(true)
		filePath := args[0]

		// 1. Load the given file
		log.Printf("Scanning %s...\n", filePath)
		f, err := os.Open(filePath)
		if err != nil {
			log.Printf("failed: %s\n", err)
		}

		// 2. Scan a few messages
		sc := scanner.NewScanner(
			cfg.DateFormat,
			regexp.MustCompile(cfg.MessageStartRE),
			10_000_000,
			100_000_000,
		)

		i := 1
		err = sc.Scan(f, func(m *scanner.ScannedMessage) bool {
			if i > 3 {
				return true
			}
			log.Printf("Message detected:\n%s\n", m.Body)
			i++
			return false
		})
		if err != nil {
			log.Printf("Failed to detect a message: %s\n", err)
			return
		}

		log.Printf("Nice, your config is good!")
	},
}

func allowConfigFlags(cmd *cobra.Command, args []string) {
	// This was tough to do: https://github.com/spf13/viper/issues/233#issuecomment-386791444
	viper.BindPFlag("StoragePath", cmd.Flags().Lookup("storage_path"))
	viper.BindPFlag("FilesGlobPattern", cmd.PersistentFlags().Lookup("files"))
	viper.BindPFlag("MessageStartRE", cmd.PersistentFlags().Lookup("message_pattern"))
	viper.BindPFlag("DateFormat", cmd.PersistentFlags().Lookup("date_format"))
}

func init() {
	configurableCommands := []*cobra.Command{initCmd, runCmd, testCmd}
	for _, cmd := range configurableCommands {
		cmd.Flags().String("storage_path", "", "storage path (default is ./)")
		cmd.PersistentFlags().String("files", "", "glob pattern to find log files (default is ./*.log)")
		cmd.PersistentFlags().String("message_pattern", "", "a RE pattern to find a message beginning, syntax RE2, see https://github.com/google/re2/wiki/Syntax")
		cmd.PersistentFlags().String("date_format", "", "a date format to parse a date in a message, see https://go.dev/src/time/format.go")
	}

	rootCmd.AddCommand(initCmd, runCmd, detectCmd, testCmd)
}

// Run resolves the app as it needs with the given procedure
func Run() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
