# Search For Local Log Files

![Heaplog logo](Heaplog.jpg)

// main branch build status

Heaplog is a program that runs in the background, scans and indexes your log files, and allows to search it via Web UI.
It aims to take small disk space and allow fast searches using its query language (see below).

## Installation

## Configuration

Configuration can be provided as a Yaml file, as well as command arguments (where the latter overwrite the former).
Configurable keys and values can be seen in [config.go](https://github.com/lezhnev74/heaplog/ui/config.go).
To populate a new empty file run `heaplog init > heaplog.yml`.

Since there are many formats of log files, you have to provide two things about your file format:
1. Regular Expression to find individual messages(config key `MessageStartRE`) in your files.
2. Date format(config key `DateFormat`) to parse its timestamps.

### Use Automatic Format Detection Command

This command `heaplog detect` will ask you to give it a sample log message. It will try to detect date format automatically.
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
Write Go code to parse this date.

[2023-12-31T00:00:03.448201+00:00] production.DEBUG: My message
```

### Provide Format Manually

The program needs a regular expression that detects the beginning of each message (see [re docs](https://pkg.go.dev/regexp/syntax)).
In the first matching group it must contain the full date of the message.
Below is the regular expression that can recognize messages and dates of this format:
```
[2023-12-31T00:00:03.448201+00:00] production.DEBUG: My message

(?m)^\[([^\]]+)
```

### Test Your Config
Once you have configured the app, run this command to make sure everything is ok:
`heaplog test <path/to/log.file>`.

## Query Language

## Design

See more about design ideas in this blog post.

## Licence

MIT