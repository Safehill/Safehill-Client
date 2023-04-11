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
        .package(url: "https://github.com/Safehill/Safehill-Crypto", from: "1.1.6"),
        .package(url: "https://github.com/gennarinoos/KnowledgeBase.git", from: "0.9.2"),
        .package(url: "https://github.com/jpsim/Yams.git", from: "5.0.5")
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "Safehill-Client",
            dependencies: [
                "Safehill-Crypto",
                "KnowledgeBase",
                "Yams"
            ]
        ),
        .testTarget(
            name: "Safehill-ClientTests",
            dependencies: ["Safehill-Client"]),
    ]
)
