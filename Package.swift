// swift-tools-version:5.5
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Safehill-Client",
    platforms: [
        .macOS(.v11), .iOS(.v14)
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "Safehill-Client",
            targets: ["Safehill-Client"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(name: "Safehill-Crypto", path: "../Safehill-Crypto"),
        .package(name: "KnowledgeBase", path: "../KnowledgeBase/KnowledgeBase")
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "Safehill-Client",
            dependencies: [
                "Safehill-Crypto",
                "KnowledgeBase",
            ]
        ),
        .testTarget(
            name: "Safehill-ClientTests",
            dependencies: ["Safehill-Client"]),
    ]
)
