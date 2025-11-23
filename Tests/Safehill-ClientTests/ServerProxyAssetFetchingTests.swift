//
//  ServerProxyAssetFetchingTests.swift
//  Safehill-Client
//
//  Created by Gennaro on 5/19/25.
//

import XCTest
@testable import Safehill_Client
import Safehill_Crypto
import CryptoKit

final class ServerProxyAssetFetchingTests: XCTestCase {
    var proxy: SHServerProxy!
    var mockLocalServer: MockLocalServer!
    var mockRemoteServer: MockRemoteServer!

    override func setUp() async throws {
        let privateKey = try P256.KeyAgreement.PrivateKey(rawRepresentation: Data(base64Encoded: "rzp6ddhcGiJmlHvmk9JJsPa+rPiI3pKOrK+bNKLBlZg=")!)
        let privateSignature = try P256.Signing.PrivateKey(rawRepresentation: Data(base64Encoded: "aRS76J3PGP+WU7+pYQCDbf18hCo5Pn5PpjGFa7erLHo=")!)
        let localUser = SHLocalCryptoUser(key: privateKey, signature: privateSignature)
        let requestor = SHLocalUser(shUser: localUser, authToken: nil, maybeEncryptionProtocolSalt: nil, keychainPrefix: "testkeychain")
        mockLocalServer = MockLocalServer(requestor: requestor, stubbedAssets: [:])
        mockRemoteServer = MockRemoteServer(requestor: requestor, stubbedDescriptors: [], stubbedAssets: [:])
        proxy = SHServerProxy(local: mockLocalServer, remote: mockRemoteServer)
    }

    func test_fetchAllFromLocal() {
        let dummyVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: Data(),
            encryptedSecret: Data(),
            publicKeyData: Data(),
            publicSignatureData: Data(),
            verificationSignatureData: Data()
        )
        let dummyAsset = DummyAsset(
            globalIdentifier: "id1",
            versions: [.lowResolution: dummyVersion]
        )
        mockLocalServer.stubbedAssets = [
            dummyAsset.globalIdentifier: dummyAsset
        ]
        mockRemoteServer.stubbedDescriptors = []

        let expectation = self.expectation(description: "fetch")
        proxy.getAssetsAndCache(withGlobalIdentifiers: [dummyAsset.globalIdentifier], versions: [.lowResolution]) { result in
            if case .success(let assets) = result {
                XCTAssertEqual(assets.count, 1)
                XCTAssertNotNil(assets[dummyAsset.globalIdentifier])
            } else {
                XCTFail("Expected success")
            }
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 1)
    }

    func test_fetchFromRemoteWhenMissing() {
        let dummyVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: Data(),
            encryptedSecret: Data(),
            publicKeyData: Data(),
            publicSignatureData: Data(),
            verificationSignatureData: Data()
        )
        let dummyAsset = DummyAsset(
            globalIdentifier: "id1",
            versions: [.lowResolution: dummyVersion]
        )
        mockLocalServer.stubbedAssets = [:]
        mockRemoteServer.stubbedDescriptors = [
            DummyDescriptor(globalIdentifier: dummyAsset.globalIdentifier)
        ]
        mockRemoteServer.stubbedAssets = [
            dummyAsset.globalIdentifier: dummyAsset
        ]

        let expectation = self.expectation(description: "remote fetch")
        proxy.getAssetsAndCache(withGlobalIdentifiers: [dummyAsset.globalIdentifier], versions: [.lowResolution]) { result in
            if case .success(let assets) = result {
                XCTAssertEqual(assets[dummyAsset.globalIdentifier]?.encryptedVersions.count, 1)
            } else {
                XCTFail("Expected remote fetch to succeed")
            }
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 1)
    }

    func test_partialLocalPartialRemote() {
        let dummyVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: Data(),
            encryptedSecret: Data(),
            publicKeyData: Data(),
            publicSignatureData: Data(),
            verificationSignatureData: Data()
        )
        let dummyAsset1 = DummyAsset(
            globalIdentifier: "id1",
            versions: [.lowResolution: dummyVersion]
        )
        let dummyAsset2 = DummyAsset(
            globalIdentifier: "id2",
            versions: [.lowResolution: dummyVersion]
        )
        mockLocalServer.stubbedAssets = [
            dummyAsset1.globalIdentifier: dummyAsset1
        ]
        mockRemoteServer.stubbedDescriptors = [
            DummyDescriptor(globalIdentifier: dummyAsset2.globalIdentifier)
        ]
        mockRemoteServer.stubbedAssets = [
            dummyAsset2.globalIdentifier: dummyAsset2
        ]

        let expectation = self.expectation(description: "partial")
        proxy.getAssetsAndCache(withGlobalIdentifiers: [dummyAsset1.globalIdentifier, dummyAsset2.globalIdentifier], versions: [.lowResolution]) { result in
            if case .success(let assets) = result {
                XCTAssertEqual(assets.count, 2)
            } else {
                XCTFail("Expected success")
            }
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 1)
    }
}
