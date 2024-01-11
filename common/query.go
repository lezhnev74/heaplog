package common

import (
	"path"
	"time"
)

// DataSourceHash this program works with a hashed value only to separate index files
type DataSourceHash string

func (d DataSourceHash) InvertedIndexRoot(root string) string {
	return path.Join(root, string(d))
}

func HashFile(filename string) DataSourceHash {
	return DataSourceHash(HashString(filename))
}

type QuerySummary struct {
	Text, QueryId     string
	From, To, BuiltAt *time.Time // time scope as given upon creation of the query (can be probably used for sub-query too)
	Complete          bool       // if the query is still in-flight
	Total             int
	MinDoc, MaxDoc    *time.Time
}

// MatchedMessage is a message matched the query criteria
type MatchedMessage struct {
	Id         int64
	Loc        Location
	Date       time.Time
	QueryHash  string
	DataSource DataSourceHash
}
