grammar QueryLanguage;

options {
    language=Go;
}

////////////////////////// PARSER ///////////////////////////////////////////////////////////

query:	expr;
expr
    :	'!' expr                        # ExprNot
    |   expr OR   expr	                # ExprOr
    |   expr AND? expr                  # ExprAnd
    |	'(' expr ')'                    # ExprGroup
    |   RE_LITERAL                  # ExprRELiteral
    |   LITERAL                         # ExprLiteral
    ;

////////////////////////////// LEXER ///////////////////////////////////////////////////////

OR options { caseInsensitive=true; }: 'OR';
AND options { caseInsensitive=true; }: 'AND';

RE_LITERAL: '~' ( LITERAL | PARENTHESES_LITERAL );
LITERAL: SQUOTED_LITERAL | DQUOTED_LITERAL | KEYWORD_LITERAL;

fragment PARENTHESES_LITERAL: '(' ~[)]+ ')';
fragment KEYWORD_LITERAL: (~[ \r\t\n!)(])+;
fragment SQUOTED_LITERAL: '\'' ~[']+ '\'';
fragment DQUOTED_LITERAL: '"' ~["]+ '"';

WS : [ \r\t\n]+ -> skip;