
#ifndef YY_TURTLE_PARSER_TURTLE_PARSER_TAB_H_INCLUDED
# define YY_TURTLE_PARSER_TURTLE_PARSER_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int turtle_parser_debug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    A = 258,
    HAT = 259,
    DOT = 260,
    COMMA = 261,
    SEMICOLON = 262,
    LEFT_SQUARE = 263,
    RIGHT_SQUARE = 264,
    LEFT_ROUND = 265,
    RIGHT_ROUND = 266,
    LEFT_CURLY = 267,
    RIGHT_CURLY = 268,
    TRUE_TOKEN = 269,
    FALSE_TOKEN = 270,
    PREFIX = 271,
    BASE = 272,
    SPARQL_PREFIX = 273,
    SPARQL_BASE = 274,
    STRING_LITERAL = 275,
    URI_LITERAL = 276,
    GRAPH_NAME_LEFT_CURLY = 277,
    BLANK_LITERAL = 278,
    QNAME_LITERAL = 279,
    IDENTIFIER = 280,
    LANGTAG = 281,
    INTEGER_LITERAL = 282,
    FLOATING_LITERAL = 283,
    DECIMAL_LITERAL = 284,
    ERROR_TOKEN = 285
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE YYSTYPE;
union YYSTYPE
{
#line 136 "./turtle_parser.y" /* yacc.c:1909  */

  unsigned char *string;
  raptor_term *identifier;
  raptor_sequence *sequence;
  raptor_uri *uri;

#line 92 "turtle_parser.tab.h" /* yacc.c:1909  */
};
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int turtle_parser_parse (raptor_parser* rdf_parser, void* yyscanner);

#endif /* !YY_TURTLE_PARSER_TURTLE_PARSER_TAB_H_INCLUDED  */
