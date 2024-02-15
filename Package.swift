// swift-tools-version:5.9
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
        .package(url: "https://github.com/Safehill/Safehill-Crypto", exact: "1.1.17"),
        .package(url: "https://github.com/gennarinoos/KnowledgeBase.git", exact: "0.9.18"),
        .package(url: "https://github.com/jpsim/Yams.git", .upToNextMajor(from: "5.0.5")),
        .package(url: "https://github.com/marmelroy/PhoneNumberKit", .upToNextMajor(from: "3.4.5"))
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "Safehill-Client",
            dependencies: [
                "Safehill-Crypto",
                "KnowledgeBase",
                "Yams",
                "PhoneNumberKit"
            ]
        ),
        .testTarget(
            name: "Safehill-ClientTests",
            dependencies: ["Safehill-Client"]
        ),
    ]
)
