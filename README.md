# Search For Local Log Files

![Heaplog logo](Heaplog.png)

[![Go](https://github.com/lezhnev74/heaplog/actions/workflows/go.yml/badge.svg)](https://github.com/lezhnev74/heaplog/actions/workflows/go.yml)

Heaplog is a program that runs in the background, scans and indexes your log files, and allows to search it via Web UI.
It aims to take small disk space and allow fast searches using its query language (see below).

It builds a small separate index (in its storage directory) from your files.
It does not contain your log messages, only some meta information to help find where the messages are, 
using your logs as "heapfiles" (hence the name). 

**Table of content:**

- [Installation](#installation)
- [Configuration](#configuration)
- [Query Language](#query-language)
- [Design Ideas](#design)
- [License](#licence)

<a href="https://github.com/lezhnev74/heaplog/blob/main/HeaplogScreenshot.png"><img src="HeaplogScreenshot.png" style="width:400px;"></a>

## Features
- Supports append-only files (logs and such).
- Supports multi-line log messages.
- Runs as a background service: exposes Web UI, runs indexing workers.
- Modest on disk space (uses [DuckDB](https://duckdb.org/) + [FST](https://blog.burntsushi.net/transducers/) for terms).
- UI timeline widget supports zooming in results.
- Query language supports fast exact matching as well as regular expressions. 

## Installation

### Docker Image

The program comes as a docker image `lezhnev74/heaplog`.
Here is a sample docker-compose config file:

```yaml
services:
  heaplog:
    image: lezhnev74/heaplog
    working_dir: /app
    volumes:
      - /host/path/to/logs:/app/logs:ro
      - /host/path/to/storage:/app/storage:rw
      - ./heaplog.yml:/app/heaplog.yml:ro
    entrypoint: [ "/heaplog", "run" ]
    ports:
      - 8393:8393
```

Assuming your `heaplog.yml` contains these lines:

```yaml
FilesGlobPattern: /app/logs/*.log # this is a path within docker image (see mounted volume) 
StoragePath: /app/storage # this is a path within docker image (see mounted volume)
```

Now you can run the program with `docker-compose up heaplog` and access the UI at `http://localhost:8393`.

### Build From Source

The program compiles in a single binary file. If you have Go compiler on your machine, follow these steps:

1. Download this repo to a directory (or clone it)
2. In the directory run `go build -o heaplog`
3. Run it with `./heaplog --help`

## Configuration

Configuration can be provided as a Yaml file, as well as command arguments (where the latter overwrite the former).
Configurable keys and values can be seen in [config.go](https://github.com/lezhnev74/heaplog/ui/config.go).
To populate a new empty file run `heaplog init > heaplog.yml`.

Since there are many formats of log files, you have to provide two things about your file format:

1. Regular Expression to find individual messages(config key `MessageStartRE`) in your files.
   See [Syntax docs](https://github.com/google/re2/wiki/Syntax).
2. Go Date Format(config key `DateFormat`) to parse timestamps. See [Syntax docs](https://go.dev/src/time/format.go).

### Use Automatic Format Detection Command

This command `heaplog detect` will ask you to give it a sample log message. It will try to detect date format
automatically.
If it succeeds, you can copy the output config values and go to testing your config.

Sample output:

```
$ heaplog detect
Enter a sample message line:
[2023-12-31T00:00:03.448201+00:00] production.DEBUG: My message
 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 Yay, the date detected above!

Config values:
MessageStartRE: "(?m)^\[(\d{4}\-\d{2}\-\d{2}\w\d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2})"
DateFormat: "2006-01-02T15:04:05.000000-07:00"
```

### Use ChatGPT

Use the power of AI to do the job for you :) Use this prompt to get a go code from where you can copy-paste the regular
expression as well as date format for parsing.

```
Detect the full timestamp in this log message. 
Write the regular expression for the date.
Write Go time layout for that date that can be used in time.Parse function.

[2023-12-31T00:00:03.448201+00:00] production.DEBUG: My message
```

The answers can vary, for example the regular expression should be in multi-line mode, so we can detect many messages.
To do that make sure it is prefixed with `(?m)`.

### Provide Format Manually

The program needs a regular expression that detects the beginning of each message (
see [re docs](https://pkg.go.dev/regexp/syntax)) and a date format.
In the first matching group it must contain the full date of the message.
Below is the regular expression that can recognize messages and dates of this format:

```
[2023-12-31T00:00:03.448201+00:00] production.DEBUG: My message

MessageStartRE: "(?m)^\[([^\]]+)"
DateFormat: "2006-01-02T15:04:05.000000-07:00"
```

### Test Your Config

Once you have configured the app, run this command to make sure everything is ok:
`heaplog test <path/to/log.file>`.

## Query Language

Query language supports exact match and regular expressions. Note that exact match uses the index to speed up the query,
while regular expression does full scan of all files. To have the best performance always add at least one exact match
term
to the query to help it narrow down the search area.

Samples:

| Query (UTF-8)                                                                 | Description                                                                                                                                             |
|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `error`                                                                       | **Case-insensitive exact match**. Will find all messages with this sequence of bytes anywhere in it.                                                    |
| `"error failure"`, the same as `'error failure'`                              | Quoted exact match. Used to provide a literal with space-like symbols.                                                                                  |
| `error failure`, the same as `failure error`, the same as `error AND failure` | Looks for the presence of both exact matches `error` and `failure`. `AND` operator is assumed for literals. **No order is preserved.**                  |
| `error OR failure`, the same as `failure OR error`                            | OR-union for exact match.                                                                                                                               |
| `(error failure) OR success`                                                  | Supports parenthesis to group literals.                                                                                                                 |
| `!error`, `!(error OR failure)`                                               | Inversion of the expression.                                                                                                                            |
| `~.*`, `~error`, `~(error \d+)`                                               | `~` - **Regular Expression** operator. Everything after `~` is used as a regular expression. Matches against every messaged. It does not use the index. |
| `report ~report\d+`                                                           | Combine exact match with the RE to use the index and improve search performance.                                                                        |

## Access Control

Heaplog does not include any access control features. That is by design. You could use it by tunneling its port to your
local machine over SSH.
Or use your existing app to authorize access and then redirect to Heaplog (example:
via [nginx internal redirect](https://nginx.org/en/docs/http/ngx_http_internal_redirect_module.html)).

## Design

Read more details about how it works in [this blog post](https://lessthan12ms.com/heaplog.html).

## Licence

MIT