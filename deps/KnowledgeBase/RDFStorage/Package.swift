// swift-tools-version:5.4.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let cSettings: [PackageDescription.CSetting] = [
    .define("IS_MODULE", to: "1"),
    .define("RAPTOR_INTERNAL"),
    .define("LIBRDFA_IN_RAPTOR"),
    .define("RDF_INTERNAL"),
    .define("LIBRDF_INTERNAL"),
    .define("FLEX_VERSION_DECIMAL", to: "00000"),
    .define("HAVE_C99_VSNPRINTF", to: "1"),
    .define("RASQAL_DECIMAL_NONE", to: "1"),
    .define("RASQAL_DIGEST_INTERNAL", to: "1"),
    .define("RASQAL_QUERY_LAQRS", to: "1"),
    .define("RASQAL_QUERY_SPARQL", to: "1"),
    .define("HAVE_DLFCN_H", to: "1"),
    .define("HAVE_ERRNO_H", to: "1"),
    .define("HAVE_FCNTL_H", to: "1"),
    .define("HAVE_FLOAT_H", to: "1"),
    .define("HAVE_GETOPT", to: "1"),
    .define("HAVE_GETOPT_H", to: "1"),
    .define("HAVE_GETOPT_LONG", to: "1"),
    .define("HAVE_GETTIMEOFDAY", to: "1"),
    .define("HAVE_INTTYPES_H", to: "1"),
    .define("HAVE_ISASCII", to: "1"),
    .define("HAVE_LIBXML_HASH_H", to: "1"),
    .define("HAVE_LIBXML_HTMLPARSER_H", to: "1"),
    .define("HAVE_LIBXML_NANOHTTP_H", to: "1"),
    .define("HAVE_LIBXML_PARSER_H", to: "1"),
    .define("HAVE_LIBXML_SAX2_H", to: "1"),
    .define("HAVE_LIBXSLT_XSLT_H", .when(platforms: [.macOS])),
    .define("HAVE_LIMITS_H", to: "1"),
    .define("HAVE_MATH_H", to: "1"),
    .define("HAVE_MEMORY_H", to: "1"),
    .define("HAVE_QSORT_R", to: "1"),
    .define("HAVE_SETJMP", to: "1"),
    .define("HAVE_SETJMP_H", to: "1"),
    .define("HAVE_STAT", to: "1"),
    .define("HAVE_STDDEF_H", to: "1"),
    .define("HAVE_STDINT_H", to: "1"),
    .define("HAVE_STDLIB_H", to: "1"),
    .define("HAVE_STRCASECMP", to: "1"),
    .define("HAVE_STRINGS_H", to: "1"),
    .define("HAVE_STRING_H", to: "1"),
    .define("HAVE_STRTOK_R", to: "1"),
    .define("HAVE_SYS_PARAM_H", to: "1"),
    .define("HAVE_SYS_STAT_H", to: "1"),
    .define("HAVE_TIME_H", to: "1"),
    .define("HAVE_SYS_TYPES_H", to: "1"),
    .define("HAVE_UNISTD_H", to: "1"),
    .define("HAVE_VASPRINTF", to: "1"),
    .define("HAVE_VSNPRINTF", to: "1"),
    .define("HAVE_XMLCTXTUSEOPTIONS", to: "1"),
    .define("HAVE_XMLSAX2INTERNALSUBSET", to: "1"),
    .define("HAVE_YAJL2", to: "1"),
    .define("HAVE_YAJL_YAJL_PARSE_H", to: "1"),
    .define("HAVE___FUNCTION__", to: "1"),
    .define("RAPTOR_LIBXML_ENTITY_ETYPE", to: "1"),
    .define("RAPTOR_LIBXML_HTML_PARSE_NONET", to: "1"),
    .define("RAPTOR_LIBXML_XMLSAXHANDLER_EXTERNALSUBSET", to: "1"),
    .define("RAPTOR_LIBXML_XMLSAXHANDLER_INITIALIZED", to: "1"),
    .define("RAPTOR_LIBXML_XML_PARSE_NONET", to: "1"),
    .define("RAPTOR_MIN_VERSION_DECIMAL", to: "20000"),
    .define("RAPTOR_PARSER_GRDDL", to: "0"),
    .define("RAPTOR_PARSER_GUESS", to: "1"),
    .define("RAPTOR_PARSER_JSON", to: "1"),
    .define("RAPTOR_PARSER_NQUADS", to: "1"),
    .define("RAPTOR_PARSER_NTRIPLES", to: "1"),
    .define("RAPTOR_PARSER_RDFA", to: "1"),
    .define("RAPTOR_PARSER_RDFXML", to: "1"),
    .define("RAPTOR_PARSER_RSS", to: "1"),
    .define("RAPTOR_PARSER_TRIG", to: "1"),
    .define("RAPTOR_PARSER_TURTLE", to: "1"),
    .define("RAPTOR_SERIALIZER_ATOM", to: "1"),
    .define("RAPTOR_SERIALIZER_DOT", to: "1"),
    .define("RAPTOR_SERIALIZER_HTML", to: "1"),
    .define("RAPTOR_SERIALIZER_JSON", to: "1"),
    .define("RAPTOR_SERIALIZER_NQUADS", to: "1"),
    .define("RAPTOR_SERIALIZER_NTRIPLES", to: "1"),
    .define("RAPTOR_SERIALIZER_RDFXML", to: "1"),
    .define("RAPTOR_SERIALIZER_RDFXML_ABBREV", to: "1"),
    .define("RAPTOR_SERIALIZER_RSS_1_0", to: "1"),
    .define("RAPTOR_SERIALIZER_TURTLE", to: "1"),
    .define("RAPTOR_VERSION_DECIMAL", to: "20015"),
    .define("RAPTOR_VERSION_MAJOR", to: "2"),
    .define("RAPTOR_VERSION_MINOR", to: "0"),
    .define("RAPTOR_VERSION_RELEASE", to: "15"),
    .define("RAPTOR_XML_LIBXML", to: "1"),
    .define("STDC_HEADERS", to: "1"),
    .define("TIME_WITH_SYS_TIME", to: "1"),
    .define("RAPTOR_DISABLE_ASSERT", to: "1"),
    .define("HAVE_U64"),
    .define("HAVE_U32"),
    .define("HAVE_BYTE"),
    .define("HAVE_CONFIG_H"),
]

let package = Package(
    name: "RDFStorage",
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "RDFStorage",
            targets: ["RDFStorage"]
        ),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "RDFStorage",
            path: "Sources/RDFStorage",
            exclude: [
                "Raptor/turtle_parser.y",
                "Rasqal/sparql_parser.y",
                "Rasqal/sparql_lexer.l",
                "Raptor/turtle_lexer.l",
                "Raptor/turtle_parser.y",
                "Raptor/parsedate.y",
                "Raptor/raptor_nfc_icu.c",
                "Raptor/raptor_nfc_test.c",
                "yajl/yajl_version.c"
            ],
            publicHeadersPath: "",
            cSettings: cSettings
        ),
        .testTarget(
            name: "CRDFStorageTests",
            dependencies: ["RDFStorage"],
            path: "Tests/CRDFStorage",
            cSettings: cSettings
        ),
        .testTarget(
            name: "RDFStorageTests",
            dependencies: ["RDFStorage"],
            path: "Tests/RDFStorage",
            cSettings: cSettings
        )
    ]
)
