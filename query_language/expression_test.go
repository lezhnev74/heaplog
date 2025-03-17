package query_language

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkToLower(b *testing.B) {
	expr, err := ParseUserQuery("error1 error2")
	require.NoError(b, err)

	m := expr.GetMatcher()
	input := "error1 error2 abcdefg abcdefg abcdefg abcdefg abcdefg abcdefg"
	for i := 0; i < b.N; i++ {
		m(NewCachedString(input))
	}
}

func TestString(t *testing.T) {
	type test struct {
		query          *Expression
		expectedString string
	}
	tests := []test{
		{
			&Expression{NOT, []any{"OP4"}},
			"NOT(OP4)",
		},
		{
			&Expression{AND, []any{
				"OP1",
				&Expression{OR, []any{"T1", "T2"}},
			}},
			"AND(OP1,OR(T1,T2))",
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			require.Equal(t, tt.expectedString, tt.query.String())
		})
	}
}

func TestMatch(t *testing.T) {

	message := `
[2023-01-05 23:46:22.234123] testing.DEBUG: BING ADS API #0:
BING ADS response (recorded):
{
    "ReportRequestStatus": {
        "ReportDownloadUrl": null,
        "Status": "Success"
    }
}
{"exec":{"label":"6f85c55a-4f23-45cc-8a3c-c814cc1a1d98","environment":"testing","started_at":1678491979534005,"user_id":null,"channel":{"type":"console"},"extras":[]}}
`

	// test how expressions can apply itself To strings for pattern matching
	type test struct {
		query         string
		expectedMatch bool
	}
	tests := []test{
		{"wrong", false},
		{"~wrong", false},
		{"!wrong", true},
		{"Report", true},
		//{"report", false}, // case-sensitive matching enabled
		{"~Report", true},
		{"~(Report)", true},
		{"~Re.{3}t", true},
		{"!Report", false},
		{"!~Re.{3}t", false},
		{"wrong OR Report", true},
		{"wrong AND Report", false},
		{"BING !DEBUG", false},
		{"BING !DEBUG !status", false},
		{"!wrong AND Report", true},
		{"wrong OR (Report AND Success)", true},
		{"wrong OR (Report AND !Success)", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := ParseUserQuery(tt.query)
			require.NoError(t, err)
			log.Print(expr.String())
			require.Equal(t, tt.expectedMatch, expr.GetMatcher()(NewCachedString(message)))
		})
	}
}

func TestFindKeywords(t *testing.T) {
	t.Run("sort expr", func(t *testing.T) {
		expr := &Expression{AND, []any{
			"Z",
			&Expression{OR, []any{
				"Y",
				"B",
			}},
			"A",
		}}
		expr.sort()
		literals := expr.FindKeywords()
		expectedLiteras := []string{"B", "Y", "A", "Z"}
		require.Equal(t, expectedLiteras, literals)
	})
}

func TestClone(t *testing.T) {
	t.Run("expr can be cloned", func(t *testing.T) {
		expr1 := &Expression{AND, []any{
			"OP1",
			&Expression{OR, []any{
				"OP3",
				"OP4",
			}},
			"OP2",
		}}
		expr2 := expr1.Clone()

		// change all literals of the original expr
		expr1.MapLiterals(func(literal any) any { return literal.(string) + "_" })

		// make sure cloned expression is not affected
		literals1 := expr1.FindKeywords()
		literals2 := expr2.FindKeywords()
		require.NotEqualValues(t, literals1, literals2)
	})
}

func TestHashing(t *testing.T) {

	t.Run("hashes differ", func(t *testing.T) {
		q1 := "error"
		q2 := "~error"

		e1, err := ParseUserQuery(q1)
		require.NoError(t, err)
		e2, err := ParseUserQuery(q2)
		require.NoError(t, err)

		require.NotEqual(t, e1.Hash(), e2.Hash())
	})

	t.Run("expr is sorted before hashing", func(t *testing.T) {
		expr1 := &Expression{AND, []any{
			"OP1",
			&Expression{OR, []any{
				"OP3",
				"OP4",
			}},
			"OP2",
		}}
		expr2 := &Expression{AND, []any{
			"OP2",
			&Expression{OR, []any{
				"OP4",
				"OP3",
			}},
			"OP1",
		}}

		hash1, hash2 := expr1.Hash(), expr2.Hash()
		require.EqualValues(t, hash1, hash2)
	})
}

func TestVisiting(t *testing.T) {
	tree := &Expression{AND, []any{
		"OP1",
		&Expression{OR, []any{
			"OP3",
			&Expression{NOT, []any{
				"OP4",
			}},
		}},
		"OP2",
	}}

	t.Run("Visit", func(t *testing.T) {
		expected := []any{"OP4", "OP3", "OP1", "OP2"} // order is implementation dependent
		actual := []any{}
		visit := func(expr *Expression) {
			for _, operand := range expr.Operands {
				if str, ok := operand.(string); ok {
					actual = append(actual, str)
				}
			}
		}
		tree.Visit(visit)

		require.EqualValues(t, expected, actual)
	})
}

func TestReplaceUnaryAndToOr(t *testing.T) {
	type test struct {
		expr, expectedResult *Expression
		optimizationExpected bool
	}
	tests := []test{
		{ // And replaced with Or
			&Expression{AND, []any{""}},
			&Expression{OR, []any{""}},
			true,
		},
		{ // empty is not replaced
			&Expression{AND, []any{}},
			&Expression{AND, []any{}},
			false,
		},
		{ // do nothing
			&Expression{AND, []any{"T1", "T2"}},
			&Expression{AND, []any{"T1", "T2"}},
			false,
		},
		{ // nested AND converted to OR
			&Expression{AND, []any{
				"T0",
				&Expression{AND, []any{"T2"}},
			}},
			&Expression{AND, []any{
				"T0",
				&Expression{OR, []any{"T2"}},
			}},
			true,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			optimized := test.expr.convertUnaryAndToOr()
			require.Equal(t, test.expectedResult, test.expr)
			require.Equal(t, test.optimizationExpected, optimized)
		})
	}
}

func TestRemoveUnaryAnd(t *testing.T) {
	type test struct {
		expr, expectedResult *Expression
		optimizationExpected bool
	}
	tests := []test{
		{ // do nothing
			&Expression{AND, []any{""}},
			&Expression{AND, []any{""}},
			false,
		},
		{ // parent AND merged
			&Expression{AND, []any{
				&Expression{AND, []any{"T1", "T2"}},
			}},
			&Expression{AND, []any{"T1", "T2"}},
			true,
		},
		{ // parent AND merged
			&Expression{AND, []any{
				"T0",
				&Expression{AND, []any{"T1", "T2"}},
			}},
			&Expression{AND, []any{
				"T0",
				&Expression{AND, []any{"T1", "T2"}},
			}},
			false,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			optimized := test.expr.removeUnaryAND()
			require.Equal(t, test.expectedResult, test.expr)
			require.Equal(t, test.optimizationExpected, optimized)
		})
	}
}

func TestMergingSimilarParent(t *testing.T) {
	type test struct {
		expr, expectedResult *Expression
		optimizationExpected bool
	}
	tests := []test{
		{ // do nothing
			&Expression{AND, []any{""}},
			&Expression{AND, []any{""}},
			false,
		},
		{ // parent AND merged
			&Expression{AND, []any{
				&Expression{AND, []any{"T1", "T2"}},
			}},
			&Expression{AND, []any{"T1", "T2"}},
			true,
		},
		{ // parent AND merged
			&Expression{AND, []any{
				"T0",
				&Expression{AND, []any{"T1", "T2"}},
			}},
			&Expression{AND, []any{"T0", "T1", "T2"}},
			true,
		},
		{ // multilevel merge
			&Expression{AND, []any{
				"T0",
				&Expression{AND, []any{"T1",
					&Expression{AND, []any{"T2"}},
				}},
			}},
			&Expression{AND, []any{"T0", "T1", "T2"}},
			true,
		},
		{ // deep child optimized
			&Expression{AND, []any{
				&Expression{OR, []any{
					"T1",
					&Expression{OR, []any{"T2"}},
				}},
			}},
			&Expression{AND, []any{
				&Expression{OR, []any{"T1", "T2"}},
			}},
			true,
		},
		{
			// ((T1 !T2) !T3) => (T1 !T2 !T3)
			// this optimization is nice to have as Antlr groups operators too much
			&Expression{AND, []any{
				&Expression{AND, []any{
					"T1",
					&Expression{NOT, []any{"T2"}},
				}},
				&Expression{NOT, []any{"T3"}},
			}},
			&Expression{AND, []any{
				"T1",
				&Expression{NOT, []any{"T2"}},
				&Expression{NOT, []any{"T3"}},
			}},
			true,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			optimized := test.expr.mergeSimilarParent()
			require.Equal(t, test.expectedResult, test.expr)
			require.Equal(t, test.optimizationExpected, optimized)
		})
	}
}

func TestGettingLiterals(t *testing.T) {
	type test struct {
		expr            *Expression
		expectedLiteral []string
	}
	tests := []test{
		{
			&Expression{AND, []any{}},
			[]string{},
		},
		{
			&Expression{AND, []any{"A", "B"}},
			[]string{"A", "B"},
		},
		{
			&Expression{AND, []any{"A",
				&Expression{AND, []any{"B", "C"}},
			}},
			[]string{"B", "C", "A"}, // order is implementation dependent
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			literals := test.expr.FindKeywords()
			require.Equal(t, test.expectedLiteral, literals)
		})
	}
}
