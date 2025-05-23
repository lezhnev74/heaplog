# Search For Local Log Files

![Heaplog logo](Heaplog.png)

[![Go](https://github.com/lezhnev74/heaplog/actions/workflows/go.yml/badge.svg)](https://github.com/lezhnev74/heaplog/actions/workflows/go.yml)

Heaplog is a program that runs in the background, scans and indexes your log files, and allows to search it via Web UI.
It aims to take small disk space and allow fast searches using its query language (see below).

It builds a small separate index (in its storage directory) from your files.
It does not contain your log messages, only some meta information to help find where the messages are,
using your logs as "heapfiles" (hence the name).

**[📌 Watch a quickstart video on Youtube](http://www.youtube.com/watch?v=fxMeAP41btY)**

**Table of content:**

- [Installation](#installation)
- [Configuration](#configuration)
- [Query Language](#query-language)
- [Design Ideas](#design)
- [License](#licence)

<a href="https://github.com/lezhnev74/heaplog/blob/main/HeaplogScreenshot.png"><img src="HeaplogScreenshot.png" style="width:400px;"></a>

## Features

- Modest on disk space (uses [DuckDB](https://duckdb.org/) + [FST](https://blog.burntsushi.net/transducers/) for terms).
- [Inverted index](https://github.com/lezhnev74/inverted_index_2)
- Powerful query language (prefix match, regular expressions, AND-, OR-, NOT-operators).
- Supports append-only files (logs and such).
- Supports multi-line log messages.
- Runs as a background service: exposes Web UI, runs indexing workers.
- Deploys as docker container

![](Heaplog_components.png)

## Query Language

Query language supports prefix match, regular expressions, and AND-OR-NOT operators. Note that prefix match uses the
index to speed up the query,
while regular expression does full scan of all files. To have the best performance always add at least one prefix match
term to the query to help it narrow down the search area.

**What is prefix match?**
Each message is split in terms, and the beginning of each term is indexed.
Example:
`Error at locahost.domain` will be split in `Error`, `at`, `locahost`, `domain`.
We can find this message by the query line `local` but not `host`. In the latter case we should use regular expression `~host`
which won't use the index, but does full-scan.

Samples:

| Query (UTF-8)                                                                 | Description                                                                                                                                             |
|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `error`                                                                       | **Case-insensitive prefix match**. Will find all messages with this sequence of bytes as term prefix.                                                   |
| `"error failure"`, the same as `'error failure'`                              | Quoted exact match. Used to provide a literal with space-like symbols.                                                                                  |
| `error failure`, the same as `failure error`, the same as `error AND failure` | Looks for the presence of both exact matches `error` and `failure`. `AND` operator is assumed for literals. **No order is preserved.**                  |
| `error OR failure`, the same as `failure OR error`                            | OR-union for exact match.                                                                                                                               |
| `(error failure) OR success`                                                  | Supports parenthesis to group literals.                                                                                                                 |
| `!error`, `!(error OR failure)`                                               | Inversion of the expression.                                                                                                                            |
| `~.*`, `~error`, `~(error \d+)`, `~"error \d+"`                               | `~` - **Regular Expression** operator. Everything after `~` is used as a regular expression. Matches against every messaged. It does not use the index. |
| `report ~report\d+`                                                           | Combine prefix match with the RE to use the index and improve search performance.                                                                       |

## Installation

### Docker Image

The program comes as a docker image `lezhnev74/heaplog`.
Here is a sample docker-compose config file:

```yaml
services:
  heaplog:
    image: lezhnev74/heaplog
    volumes:
      - /host/path/to/logs:/logs:ro
      - /host/path/to/storage:/storage:rw
      - /host/path/to/heaplog.yml:/heaplog.yml:ro
    entrypoint: [ "/heaplog" ]
    command: ["run"]
    ports:
      - 8393:8393
```

Now you can run `docker compose up heaplog` and access the UI at `http://localhost:8393`.

## Configuration

Configuration can be provided as a Yaml file, as well as command arguments (where the latter overwrite the former).
Configurable keys and values can be seen in [config.go](https://github.com/lezhnev74/heaplog/blob/main/ui/config.go).
To populate a new empty file run `docker compose run heaplog gen > heaplog.yml`.

Since there are many formats of log files, you have to provide two settings:

1. Regular Expression to find individual messages(config key `MessageStartRE`) in your files.
2. Go Date Format(config key `DateFormat`) to parse timestamps. See [Syntax docs](https://go.dev/src/time/format.go).

**Example Config For PHP App Based On Laravel Framework**
```yaml
# where to look for log files? example: "./*.log"
filesglobpattern: /logs/*.log
# where to store the index and other data (relative to cwd supported)
storagepath: ./storage
# a regular expression to find the start of messages in a heap file,
# it must contain the date pattern in the first matching group
# example: "^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\]"
messagestartre: ^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2})\]
# the pattern of a date in a message
# see https://go.dev/src/time/format.go
dateformat: "2006-01-02T15:04:05.000000-07:00"
# sets the degree of concurrency in the service (affects ingestion and search),
# defaults to the number of cores if omitted or <1.
concurrency: 8
# Terms are extracted from messages and indexed.
# These control how fast ingestion goes (and space taken for the inverted index),
# as well as how fast search goes (as shorter terms may duplicate in the index).
mintermlen: 4
maxtermlen: 8
# Max memory the duckdb instance is allowed to allocate in Mb.
# Increase if you see related errors on big data sets. (default: 500)
duckdbmaxmemmb: 1000
```

### Automatic Format Detection Command

This command `docker compose run --rm heaplog detect` will ask you to give it a sample log message. 
It will try to detect the date format automatically.
If it succeeds, you can copy the output config values and paste into your config.

Sample output:
```
$ docker compose run --rm heaplog detect
Enter a sample message line:
[2023-12-31T00:00:03.448201+00:00] production.DEBUG: My message
 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 Yay, the date detected above!

Config values:
MessageStartRE: "(?m)^\[(\d{4}\-\d{2}\-\d{2}\w\d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2})"
DateFormat: "2006-01-02T15:04:05.000000-07:00"
```

### Use ChatGPT To Detect Format

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

MessageStartRE: "^\[([^\]]+)"
DateFormat: "2006-01-02T15:04:05.000000-07:00"
```

### Test Your Config

Once you have configured the app, run this command to make sure everything is ok:
`docker compose run test`.

## Access Control

Heaplog does not include any access control features. That is by design. You could use it by tunneling its port to your
local machine over SSH.
Or use your existing app to authorize access and then redirect to Heaplog (example:
via [nginx internal redirect](https://nginx.org/en/docs/http/ngx_http_internal_redirect_module.html)).

## Design

Read more details about how it works in [this blog post](https://lessthan12ms.com/heaplog.html).

## Licence

MIT