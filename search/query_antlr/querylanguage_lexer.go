// Code generated from /home/dmitry/Code/go/src/heaplog2/search/QueryLanguage.g4 by ANTLR 4.13.1. DO NOT EDIT.

package query_antlr

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"sync"
	"unicode"
)

// Suppress unused import error
var _ = fmt.Printf
var _ = sync.Once{}
var _ = unicode.IsLetter

type QueryLanguageLexer struct {
	*antlr.BaseLexer
	channelNames []string
	modeNames    []string
	// TODO: EOF string
}

var QueryLanguageLexerLexerStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	ChannelNames           []string
	ModeNames              []string
	LiteralNames           []string
	SymbolicNames          []string
	RuleNames              []string
	PredictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func querylanguagelexerLexerInit() {
	staticData := &QueryLanguageLexerLexerStaticData
	staticData.ChannelNames = []string{
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN",
	}
	staticData.ModeNames = []string{
		"DEFAULT_MODE",
	}
	staticData.LiteralNames = []string{
		"", "'!'", "'('", "')'",
	}
	staticData.SymbolicNames = []string{
		"", "", "", "", "OR", "AND", "RE_LITERAL", "LITERAL", "WS",
	}
	staticData.RuleNames = []string{
		"T__0", "T__1", "T__2", "OR", "AND", "RE_LITERAL", "LITERAL", "PARENTHESES_LITERAL",
		"KEYWORD_LITERAL", "SQUOTED_LITERAL", "DQUOTED_LITERAL", "WS",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 0, 8, 84, 6, -1, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2,
		4, 7, 4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2,
		10, 7, 10, 2, 11, 7, 11, 1, 0, 1, 0, 1, 1, 1, 1, 1, 2, 1, 2, 1, 3, 1, 3,
		1, 3, 1, 4, 1, 4, 1, 4, 1, 4, 1, 5, 1, 5, 1, 5, 3, 5, 42, 8, 5, 1, 6, 1,
		6, 1, 6, 3, 6, 47, 8, 6, 1, 7, 1, 7, 4, 7, 51, 8, 7, 11, 7, 12, 7, 52,
		1, 7, 1, 7, 1, 8, 4, 8, 58, 8, 8, 11, 8, 12, 8, 59, 1, 9, 1, 9, 4, 9, 64,
		8, 9, 11, 9, 12, 9, 65, 1, 9, 1, 9, 1, 10, 1, 10, 4, 10, 72, 8, 10, 11,
		10, 12, 10, 73, 1, 10, 1, 10, 1, 11, 4, 11, 79, 8, 11, 11, 11, 12, 11,
		80, 1, 11, 1, 11, 0, 0, 12, 1, 1, 3, 2, 5, 3, 7, 4, 9, 5, 11, 6, 13, 7,
		15, 0, 17, 0, 19, 0, 21, 0, 23, 8, 1, 0, 10, 2, 0, 79, 79, 111, 111, 2,
		0, 82, 82, 114, 114, 2, 0, 65, 65, 97, 97, 2, 0, 78, 78, 110, 110, 2, 0,
		68, 68, 100, 100, 1, 0, 41, 41, 4, 0, 9, 10, 13, 13, 32, 33, 40, 41, 1,
		0, 39, 39, 1, 0, 34, 34, 3, 0, 9, 10, 13, 13, 32, 32, 87, 0, 1, 1, 0, 0,
		0, 0, 3, 1, 0, 0, 0, 0, 5, 1, 0, 0, 0, 0, 7, 1, 0, 0, 0, 0, 9, 1, 0, 0,
		0, 0, 11, 1, 0, 0, 0, 0, 13, 1, 0, 0, 0, 0, 23, 1, 0, 0, 0, 1, 25, 1, 0,
		0, 0, 3, 27, 1, 0, 0, 0, 5, 29, 1, 0, 0, 0, 7, 31, 1, 0, 0, 0, 9, 34, 1,
		0, 0, 0, 11, 38, 1, 0, 0, 0, 13, 46, 1, 0, 0, 0, 15, 48, 1, 0, 0, 0, 17,
		57, 1, 0, 0, 0, 19, 61, 1, 0, 0, 0, 21, 69, 1, 0, 0, 0, 23, 78, 1, 0, 0,
		0, 25, 26, 5, 33, 0, 0, 26, 2, 1, 0, 0, 0, 27, 28, 5, 40, 0, 0, 28, 4,
		1, 0, 0, 0, 29, 30, 5, 41, 0, 0, 30, 6, 1, 0, 0, 0, 31, 32, 7, 0, 0, 0,
		32, 33, 7, 1, 0, 0, 33, 8, 1, 0, 0, 0, 34, 35, 7, 2, 0, 0, 35, 36, 7, 3,
		0, 0, 36, 37, 7, 4, 0, 0, 37, 10, 1, 0, 0, 0, 38, 41, 5, 126, 0, 0, 39,
		42, 3, 13, 6, 0, 40, 42, 3, 15, 7, 0, 41, 39, 1, 0, 0, 0, 41, 40, 1, 0,
		0, 0, 42, 12, 1, 0, 0, 0, 43, 47, 3, 19, 9, 0, 44, 47, 3, 21, 10, 0, 45,
		47, 3, 17, 8, 0, 46, 43, 1, 0, 0, 0, 46, 44, 1, 0, 0, 0, 46, 45, 1, 0,
		0, 0, 47, 14, 1, 0, 0, 0, 48, 50, 5, 40, 0, 0, 49, 51, 8, 5, 0, 0, 50,
		49, 1, 0, 0, 0, 51, 52, 1, 0, 0, 0, 52, 50, 1, 0, 0, 0, 52, 53, 1, 0, 0,
		0, 53, 54, 1, 0, 0, 0, 54, 55, 5, 41, 0, 0, 55, 16, 1, 0, 0, 0, 56, 58,
		8, 6, 0, 0, 57, 56, 1, 0, 0, 0, 58, 59, 1, 0, 0, 0, 59, 57, 1, 0, 0, 0,
		59, 60, 1, 0, 0, 0, 60, 18, 1, 0, 0, 0, 61, 63, 5, 39, 0, 0, 62, 64, 8,
		7, 0, 0, 63, 62, 1, 0, 0, 0, 64, 65, 1, 0, 0, 0, 65, 63, 1, 0, 0, 0, 65,
		66, 1, 0, 0, 0, 66, 67, 1, 0, 0, 0, 67, 68, 5, 39, 0, 0, 68, 20, 1, 0,
		0, 0, 69, 71, 5, 34, 0, 0, 70, 72, 8, 8, 0, 0, 71, 70, 1, 0, 0, 0, 72,
		73, 1, 0, 0, 0, 73, 71, 1, 0, 0, 0, 73, 74, 1, 0, 0, 0, 74, 75, 1, 0, 0,
		0, 75, 76, 5, 34, 0, 0, 76, 22, 1, 0, 0, 0, 77, 79, 7, 9, 0, 0, 78, 77,
		1, 0, 0, 0, 79, 80, 1, 0, 0, 0, 80, 78, 1, 0, 0, 0, 80, 81, 1, 0, 0, 0,
		81, 82, 1, 0, 0, 0, 82, 83, 6, 11, 0, 0, 83, 24, 1, 0, 0, 0, 8, 0, 41,
		46, 52, 59, 65, 73, 80, 1, 6, 0, 0,
	}
	deserializer := antlr.NewATNDeserializer(nil)
	staticData.atn = deserializer.Deserialize(staticData.serializedATN)
	atn := staticData.atn
	staticData.decisionToDFA = make([]*antlr.DFA, len(atn.DecisionToState))
	decisionToDFA := staticData.decisionToDFA
	for index, state := range atn.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(state, index)
	}
}

// QueryLanguageLexerInit initializes any static state used to implement QueryLanguageLexer. By default the
// static state used to implement the lexer is lazily initialized during the first call to
// NewQueryLanguageLexer(). You can call this function if you wish to initialize the static state ahead
// of time.
func QueryLanguageLexerInit() {
	staticData := &QueryLanguageLexerLexerStaticData
	staticData.once.Do(querylanguagelexerLexerInit)
}

// NewQueryLanguageLexer produces a new lexer instance for the optional input antlr.CharStream.
func NewQueryLanguageLexer(input antlr.CharStream) *QueryLanguageLexer {
	QueryLanguageLexerInit()
	l := new(QueryLanguageLexer)
	l.BaseLexer = antlr.NewBaseLexer(input)
	staticData := &QueryLanguageLexerLexerStaticData
	l.Interpreter = antlr.NewLexerATNSimulator(l, staticData.atn, staticData.decisionToDFA, staticData.PredictionContextCache)
	l.channelNames = staticData.ChannelNames
	l.modeNames = staticData.ModeNames
	l.RuleNames = staticData.RuleNames
	l.LiteralNames = staticData.LiteralNames
	l.SymbolicNames = staticData.SymbolicNames
	l.GrammarFileName = "QueryLanguage.g4"
	// TODO: l.EOF = antlr.TokenEOF

	return l
}

// QueryLanguageLexer tokens.
const (
	QueryLanguageLexerT__0       = 1
	QueryLanguageLexerT__1       = 2
	QueryLanguageLexerT__2       = 3
	QueryLanguageLexerOR         = 4
	QueryLanguageLexerAND        = 5
	QueryLanguageLexerRE_LITERAL = 6
	QueryLanguageLexerLITERAL    = 7
	QueryLanguageLexerWS         = 8
)
