package query_language

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAntlrQueryParser(t *testing.T) {

	type test struct {
		input         string
		expected      *Expression
		expectedError error
	}

	tests := []test{
		{"", &Expression{AND, []any{}}, nil},
		{"A OR", nil, errorUserQueryInvalidSyntax},
		{"(A", nil, errorUserQueryInvalidSyntax},
		{"A", &Expression{AND, []any{"A"}}, nil},
		{`192.168.0.2`, &Expression{AND, []any{"192.168.0.2"}}, nil},
		{`@$`, &Expression{AND, []any{"@$"}}, nil},
		{`'A' "B"`, &Expression{AND, []any{"A", "B"}}, nil},
		{`AA BB`, &Expression{AND, []any{"AA", "BB"}}, nil},
		{"!A", &Expression{NOT, []any{"A"}}, nil},
		{"!A !B", &Expression{AND, []any{&Expression{NOT, []any{"A"}}, &Expression{NOT, []any{"B"}}}}, nil},
		{"A B", &Expression{AND, []any{"A", "B"}}, nil},
		{"A And B", &Expression{AND, []any{"A", "B"}}, nil},
		{"(A) (B C)", &Expression{AND, []any{"A", "B", "C"}}, nil},
		{"A Or B OR C", &Expression{OR, []any{"A", "B", "C"}}, nil},
		{"A OR (B AND C)", &Expression{OR, []any{"A", &Expression{AND, []any{"B", "C"}}}}, nil},
		{"A B OR C", &Expression{AND, []any{"A", &Expression{OR, []any{"B", "C"}}}}, nil},
		{"A !B !C", &Expression{AND, []any{"A", &Expression{NOT, []any{"B"}}, &Expression{NOT, []any{"C"}}}}, nil},
		// RE:
		{"~a", &Expression{AND, []any{RegExpLiteral("a")}}, nil},
		{"~(a)", &Expression{AND, []any{RegExpLiteral("(a)")}}, nil},
		{"~[ab]+", &Expression{AND, []any{RegExpLiteral("[ab]+")}}, nil},
		{"~[ab]+ qwe", &Expression{AND, []any{RegExpLiteral("[ab]+"), "qwe"}}, nil},
		{"a ~a", &Expression{AND, []any{"a", RegExpLiteral("a")}}, nil},
		{"!~a", &Expression{NOT, []any{RegExpLiteral("a")}}, nil},
		{"a OR !~a", &Expression{OR, []any{"a", &Expression{NOT, []any{RegExpLiteral("a")}}}}, nil},
	}

	for _, ti := range tests {
		t.Run(
			ti.input, func(t *testing.T) {
				qe, err := ParseUserQuery(ti.input)
				if err != nil {
					require.ErrorContains(t, err, ti.expectedError.Error())
				}
				qe = qe.optimize()
				log.Printf("exp: %s", ti.expected.String())
				log.Printf("act: %s", qe.String())
				require.Equal(t, ti.expected, qe)
			},
		)
	}
}
