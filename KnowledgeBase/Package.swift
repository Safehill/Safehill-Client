// swift-tools-version:5.4
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let cSettings: [PackageDescription.CSetting] = []

let package: Package

if #available(iOS 15, macOS 13, tvOS 15, watchOS 8, *) {
    package = Package(
        name: "KnowledgeBase",
        platforms: [
            .macOS("12.0"), .iOS("15.0"), .tvOS("15.0"), .watchOS("8.0")
        ],
        products: [
            // Products define the executables and libraries a package produces, and make them visible to other packages.
            .library(
                name: "KnowledgeBase",
                targets: ["KnowledgeBase"]),
        ],
        dependencies: [
//            // Dependencies declare other packages that this package depends on.
//            .package(name: "RDFStorage", path: "../RDFStorage")
        ],
        targets: [
            // Targets are the basic building blocks of a package. A target can define a module or a test suite.
            // Targets can depend on other targets in this package, and on products in packages this package depends on.
            .target(
                name: "KnowledgeBase",
                dependencies: [
//                    "RDFStorage"
                ],
                exclude: [
                    "Storage/Protocols",
                    "APIs",
                    "Serialization",
                    // TODO: Re-enable SPARQL endpoint
                    "SPARQL",
                    "APIs_Swift_5_5/SPARQLAPI.swift",
                    // TODO: Still need to be ported
                    "Indexers",
                    "Rules",
                    "Models/Event",
                    "Syncing"
                ],
                cSettings: cSettings
            ),
            .testTarget(
                name: "KnowledgeBaseTests_Swift_5_5",
                dependencies: ["KnowledgeBase"],
                cSettings: cSettings
            )
        ]
    )
} else {
    package = Package(
        name: "KnowledgeBase",
        platforms: [
            .macOS(.v11), .iOS(.v14), .tvOS(.v11), .watchOS(.v4)
        ],
        products: [
            // Products define the executables and libraries a package produces, and make them visible to other packages.
            .library(
                name: "KnowledgeBase",
                targets: ["KnowledgeBase"]
            )
        ],
        dependencies: [
            // Dependencies declare other packages that this package depends on.
//            .package(name: "RDFStorage", path: "../RDFStorage")
            
        ],
        targets: [
            // Targets are the basic building blocks of a package. A target can define a module or a test suite.
            // Targets can depend on other targets in this package, and on products in packages this package depends on.
            .target(
                name: "KnowledgeBase",
                dependencies: [
//                    "RDFStorage"
                ],
                exclude: [
                    "Storage_Swift_5_5/Protocols",
                    "APIs_Swift_5_5",
                    "Serialization_Swift_5_5",
                    // TODO: Re-enable SPARQL endpoint
                    "SPARQL",
                    "APIs/SPARQLAPI.swift",
                    // TODO: Still need to be ported
                    "Rules",
                    "Models/Event"
                ],
                cSettings: cSettings
            ),
            .executableTarget(
                name: "KnowledgeBaseXPCService",
                dependencies: [
                    "KnowledgeBase"
                ],
                cSettings: cSettings
            ),
            .testTarget(
                name: "KnowledgeBaseTests",
                dependencies: ["KnowledgeBase"],
                exclude: [
                    // TODO: Re-enable SPARQL endpoint
                    "SPARQLTestCase.swift"
                ],
                cSettings: cSettings
            )
        ]
    )
}
