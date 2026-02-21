/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

grammar IcebergSQL;

// ─── Entry Point ─────────────────────────────────────────────────────────────

query
    : SELECT columnList
      FROM tableRef
      (WHERE predicate)?
      (LIMIT INTEGER_LITERAL)?
      EOF
    ;

// ─── Column Projection ───────────────────────────────────────────────────────

columnList
    : STAR                              # allColumns
    | column (COMMA column)*            # namedColumns
    ;

column
    : ID (DOT ID)?                      # simpleColumn    // e.g.  region  or  t.region
    ;

// ─── Table Reference ─────────────────────────────────────────────────────────

// Supports unqualified (events), single-namespace (logs.events),
// or multi-level namespace (prod.logs.events)
tableRef
    : ID (DOT ID)*
    ;

// ─── Predicates ──────────────────────────────────────────────────────────────

predicate
    : LPAREN predicate RPAREN           # parenPred
    | NOT predicate                     # notPred
    | predicate AND predicate           # andPred
    | predicate OR predicate            # orPred
    | expression IS NOT? NULL           # isNullPred
    | expression IN
        LPAREN literal (COMMA literal)* RPAREN  # inPred
    | expression NOT IN
        LPAREN literal (COMMA literal)* RPAREN  # notInPred
    | expression op expression          # comparisonPred
    ;

// ─── Expressions & Literals ──────────────────────────────────────────────────

// An expression in a comparison is always a column reference or a literal
expression
    : ID                                # columnRef
    | literal                           # literalExpr
    ;

literal
    : INTEGER_LITERAL                   # intLiteral
    | FLOAT_LITERAL                     # floatLiteral
    | STRING_LITERAL                    # stringLiteral
    | TRUE                              # trueLiteral
    | FALSE                             # falseLiteral
    ;

// ─── Comparison Operators ────────────────────────────────────────────────────

op  : EQ | NEQ | LT | LTE | GT | GTE ;

// ─── Keywords (case-insensitive via fragment) ─────────────────────────────────

SELECT  : S E L E C T ;
FROM    : F R O M ;
WHERE   : W H E R E ;
LIMIT   : L I M I T ;
AND     : A N D ;
OR      : O R ;
NOT     : N O T ;
IN      : I N ;
IS      : I S ;
AS      : A S ;
NULL    : N U L L ;
TRUE    : T R U E ;
FALSE   : F A L S E ;

// ─── Operators & Punctuation ─────────────────────────────────────────────────

EQ      : '=' ;
NEQ     : '!=' | '<>' ;
LT      : '<' ;
LTE     : '<=' ;
GT      : '>' ;
GTE     : '>=' ;
STAR    : '*' ;
COMMA   : ',' ;
DOT     : '.' ;
LPAREN  : '(' ;
RPAREN  : ')' ;

// ─── Identifiers & Literals ──────────────────────────────────────────────────

ID
    : [a-zA-Z_] [a-zA-Z_0-9]*
    ;

INTEGER_LITERAL
    : '-'? [0-9]+
    ;

FLOAT_LITERAL
    : '-'? [0-9]+ '.' [0-9]*
    | '-'? '.' [0-9]+
    ;

// Single-quoted strings: 'us-east-1'  ('' = escaped single quote inside)
STRING_LITERAL
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

// ─── Whitespace & Comments ───────────────────────────────────────────────────

WS          : [ \t\r\n]+  -> skip ;
LINE_COMMENT : '--' ~[\r\n]* -> skip ;
BLOCK_COMMENT: '/*' .*? '*/' -> skip ;

// ─── Case-insensitive letter fragments ───────────────────────────────────────

fragment A : [aA]; fragment B : [bB]; fragment C : [cC]; fragment D : [dD];
fragment E : [eE]; fragment F : [fF]; fragment G : [gG]; fragment H : [hH];
fragment I : [iI]; fragment J : [jJ]; fragment K : [kK]; fragment L : [lL];
fragment M : [mM]; fragment N : [nN]; fragment O : [oO]; fragment P : [pP];
fragment Q : [qQ]; fragment R : [rR]; fragment S : [sS]; fragment T : [tT];
fragment U : [uU]; fragment V : [vV]; fragment W : [wW]; fragment X : [xX];
fragment Y : [yY]; fragment Z : [zZ];