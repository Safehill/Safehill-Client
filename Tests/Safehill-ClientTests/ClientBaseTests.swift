import XCTest
@testable import Safehill_Client
@testable import Safehill_Crypto
import CryptoKit
import KnowledgeBase


let kTestStaticProtocolSalt = Data(base64Encoded: "0PT/RKOwUpk8dxYU/pJ3Vx/zespMkey8yMMgFp4ov2E=")!

final class Safehill_ClientBaseUnitTests: XCTestCase {
    
    func testSubtract() throws {
        let first = ["Alice", "Bob", "Cindy"]
        let second = ["Bob", "Mary"]
        
        let subtract = first.subtract(second)
        XCTAssert(subtract.count == 2)
        XCTAssert(subtract.contains("Alice"))
        XCTAssert(subtract.contains("Cindy"))
        XCTAssert(!subtract.contains("Bob"))
        XCTAssert(!subtract.contains("Mary"))
    }
    
    func testCircuitBreaker() throws {
        let expectation = expectation(description: "cb")
        
        let maxTimeout = 5.0
        
        let start = CFAbsoluteTimeGetCurrent()
        
        let circuitBreaker = CircuitBreaker(
            timeout: maxTimeout,
            maxRetries: 10,
            timeBetweenRetries: 0.5,
            exponentialBackoff: true,
            resetTimeout: 20.0
        )
        
        circuitBreaker.call = { circuitBreaker in
            let end = CFAbsoluteTimeGetCurrent()
            if CFAbsoluteTime(end - start) < 3.0 {
                circuitBreaker.failure()
                print("failure")
            } else {
                circuitBreaker.success()
                expectation.fulfill()
                print("success")
            }
        }
        
        circuitBreaker.didTrip = { circuitBreaker, error in
            XCTFail("didTrip \(error?.localizedDescription ?? "")")
            expectation.fulfill()
        }
        
        circuitBreaker.execute()
        
        wait(for: [expectation], timeout: maxTimeout * 2)
    }
    
    func testInitKGWithRetries() throws {
        let expectation = expectation(description: "kg")
        
        DispatchQueue.global(qos: .userInteractive).async {
            KBKVStore.initKVStoreWithRetries(dbName: "com.gf.safehill.KnowledgeGraph") { result in
                if case .success(_) = result {
                    // OK
                    expectation.fulfill()
                } else {
                    XCTFail("Failed to initialize")
                }
            }
        }
        
        wait(for: [expectation], timeout: 5.0)
    }
        
    func testInitQueue() throws {
        let _ = try BackgroundOperationQueue.of(type: .fetch)
        let _ = try BackgroundOperationQueue.of(type: .encryption)
        let _ = try BackgroundOperationQueue.of(type: .upload)
        let _ = try BackgroundOperationQueue.of(type: .share)
        let _ = try BackgroundOperationQueue.of(type: .successfulUpload)
        let _ = try BackgroundOperationQueue.of(type: .successfulShare)
        let _ = try BackgroundOperationQueue.of(type: .failedUpload)
        let _ = try BackgroundOperationQueue.of(type: .failedShare)
        let _ = try BackgroundOperationQueue.of(type: .download)
    }
}

final class Safehill_ClientEncryptionUnitTests: XCTestCase {
    
    func testSelfSharing() throws {
        let imageData = "test".data(using: .utf8)! // UIImage(named: "Logo")!.pngData()! // UIImage(named: "Logo", in: Bundle(for: Safehill_ClientTests.self), compatibleWith: nil)

        /// Local User A
        let aPrivateKey = try P256.KeyAgreement.PrivateKey(rawRepresentation: Data(base64Encoded: "rzp6ddhcGiJmlHvmk9JJsPa+rPiI3pKOrK+bNKLBlZg=")!)
        let aPrivateSignature = try P256.Signing.PrivateKey(rawRepresentation: Data(base64Encoded: "aRS76J3PGP+WU7+pYQCDbf18hCo5Pn5PpjGFa7erLHo=")!)
        let aLocalUser = SHLocalCryptoUser(key: aPrivateKey, signature: aPrivateSignature)
        
        /// Remote user A (same user)
        let aRemoteUser = try SHRemoteCryptoUser(publicKeyData: aPrivateKey.publicKey.derRepresentation, publicSignatureData: aPrivateSignature.publicKey.derRepresentation)
        
        /// A encrypts image for A
        let encryptedImage = try SHEncryptedData(clearData: imageData)
        let encryptedImageSecret = try SHUserContext(user: aLocalUser)
            .shareable(data: encryptedImage.privateSecret.rawRepresentation,
                       protocolSalt: kTestStaticProtocolSalt,
                       with: aRemoteUser)
        
        let encryptedVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: encryptedImage.encryptedData,
            encryptedSecret: encryptedImageSecret.cyphertext,
            publicKeyData: encryptedImageSecret.ephemeralPublicKeyData,
            publicSignatureData: encryptedImageSecret.signature
        )
        
        let encryptedAsset = SHGenericEncryptedAsset(
            globalIdentifier: "Logo-globalId",
            localIdentifier: "Logo-localId",
            creationDate: Date(),
            encryptedVersions: [.lowResolution: encryptedVersion]
        )
        
        let version = encryptedAsset.encryptedVersions[.lowResolution]!
        let sharedSecret = SHShareablePayload(
            ephemeralPublicKeyData: version.publicKeyData,
            cyphertext: version.encryptedSecret,
            signature: version.publicSignatureData
        )
        
        let decryptedSecretData = try SHCypher.decrypt(
            sharedSecret,
            encryptionKey: aPrivateKey,
            protocolSalt: kTestStaticProtocolSalt,
            signedBy: aPrivateSignature.publicKey
        )
        XCTAssertEqual(decryptedSecretData, encryptedImage.privateSecret.rawRepresentation)
        
        let decryptedImage = try SHUserContext(user: aLocalUser).decrypt(
            version.encryptedData,
            usingEncryptedSecret: sharedSecret,
            protocolSalt: kTestStaticProtocolSalt,
            receivedFrom: aRemoteUser
        )
        
        XCTAssertEqual(imageData, decryptedImage)
    }
    
    func testSharing() throws {
        let imageData = "test".data(using: .utf8)! // UIImage(named: "Logo")!.pngData()! // UIImage(named: "Logo", in: Bundle(for: Safehill_ClientTests.self), compatibleWith: nil)
        
        /// Local User A
        let aPrivateKeyData = Data(base64Encoded: "rzp6ddhcGiJmlHvmk9JJsPa+rPiI3pKOrK+bNKLBlZg=")!
        let aPrivateSignatureData = Data(base64Encoded: "aRS76J3PGP+WU7+pYQCDbf18hCo5Pn5PpjGFa7erLHo=")!
        let aPrivateKey = try P256.KeyAgreement.PrivateKey(rawRepresentation: aPrivateKeyData)
        let aPrivateSignature = try P256.Signing.PrivateKey(rawRepresentation: aPrivateSignatureData)
        let aLocaluser = SHLocalCryptoUser(key: aPrivateKey, signature: aPrivateSignature)
        
        /// Remote user B
        let bPublicKeyData = Data(base64Encoded: "OQHp1hTpZcmFVI+J/OskCpMtSwd0osxeYJpSHRPy1zk8WyF9TqPpRPMXHNSCzOk2HSPKqa3hCuFevnItOS3WGQ==")!
        let bPublicSignatureData = Data(base64Encoded: "UJjWbMzuHtzhLzu9Mbh0S9fxKeTYYVaAzz1JJs3jR2A7tbir6H31Ub/oNhZg0vDtb6u78sQ4UGuofgo59VphPA==")!
        let bRemoteUser = try SHRemoteCryptoUser(publicKeyData: bPublicKeyData, publicSignatureData: bPublicSignatureData)
        
        /// A encrypts image for B
        let encryptedImage = try SHEncryptedData(clearData: imageData)
        
        let encryptedImageSecret = try SHUserContext(user: aLocaluser)
            .shareable(data: encryptedImage.privateSecret.rawRepresentation,
                       protocolSalt: kTestStaticProtocolSalt,
                       with: bRemoteUser)
        
        let encryptedVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: encryptedImage.encryptedData,
            encryptedSecret: encryptedImageSecret.cyphertext,
            publicKeyData: encryptedImageSecret.ephemeralPublicKeyData,
            publicSignatureData: encryptedImageSecret.signature
        )
    
        /// Local User B
        let bPrivateKeyData = Data(base64Encoded: "RwAv5t/YxS1KjxAJETjqD3qktKJzRqYYnOb3Zq2Gg9M=")!
        let bPrivateSignatureData = Data(base64Encoded: "Uwqn8XHhV8wk2UQasF7OoyvTUe8YstnnVVz5LN+4KmA=")!
        let bPrivateKey = try P256.KeyAgreement.PrivateKey(rawRepresentation: bPrivateKeyData)
        let bPrivateSignature = try P256.Signing.PrivateKey(rawRepresentation: bPrivateSignatureData)
        let bLocalUser = SHLocalCryptoUser(key: bPrivateKey, signature: bPrivateSignature)
        
        /// Remote user A
        let aPublicKeyData = Data(base64Encoded: "8Kj1Zytcj3her3x+jo85mu5PcJp9aXFzxaTPzwGFFNJTQS5XctS3jBidJqnWO0nq9Nkjdffs1RDdKpAH789n/w==")!
        let aPublicSignatureData = Data(base64Encoded: "jmYWXKWAmjXok6euGMOH3O+FptvsDWi764ibeDdn6ZhLqnDlPIvCesp7+WyL1GAZm3bniRV4QQvLwlKmHGAniw==")!
        let aRemoteUser = try SHRemoteCryptoUser(publicKeyData: aPublicKeyData, publicSignatureData: aPublicSignatureData)
        
        let encryptedSecret = SHShareablePayload(
            ephemeralPublicKeyData: encryptedVersion.publicKeyData,
            cyphertext: encryptedVersion.encryptedSecret,
            signature: encryptedVersion.publicSignatureData
        )
        let decryptedSecretData = try SHCypher.decrypt(
            encryptedSecret,
            encryptionKeyData: bPrivateKeyData,
            protocolSalt: kTestStaticProtocolSalt,
            from: aPublicSignatureData
        )
        
        XCTAssertEqual(decryptedSecretData, encryptedImage.privateSecret.rawRepresentation)
        
        let decryptedData = try SHUserContext(user: bLocalUser).decrypt(
            encryptedVersion.encryptedData,
            usingEncryptedSecret: encryptedSecret,
            protocolSalt: kTestStaticProtocolSalt,
            receivedFrom: aRemoteUser
        )
        
        XCTAssertEqual(imageData, decryptedData)
        
        /// B encrypts image for B (same user)
        /// User A should fail decryption should fail with `authenticationError`
        
        let secondEncryptedImage = try SHEncryptedData(clearData: imageData)
        let secondEncryptedImageSecret = try SHUserContext(user: bLocalUser)
            .shareable(data: encryptedImage.privateSecret.rawRepresentation,
                       protocolSalt: kTestStaticProtocolSalt,
                       with: bRemoteUser)
        
        let secondEncryptedVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: secondEncryptedImage.encryptedData,
            encryptedSecret: secondEncryptedImageSecret.cyphertext,
            publicKeyData: secondEncryptedImageSecret.ephemeralPublicKeyData,
            publicSignatureData: secondEncryptedImageSecret.signature
        )
        
        let secondSharedSecret = SHShareablePayload(
            ephemeralPublicKeyData: secondEncryptedVersion.publicKeyData,
            cyphertext: secondEncryptedVersion.encryptedSecret,
            signature: secondEncryptedVersion.publicSignatureData
        )
        
        do {
            let _ = try SHUserContext(user: aLocaluser).decrypt(
                secondEncryptedVersion.encryptedData,
                usingEncryptedSecret: secondSharedSecret,
                protocolSalt: kTestStaticProtocolSalt,
                receivedFrom: bRemoteUser
            )
            XCTFail()
        }
        catch SHCypher.DecryptionError.authenticationError {}
        catch {
            XCTFail()
        }
    }
}

final class Safehill_ClientIntegrationTests: XCTestCase {
    
    var user = SHLocalUser(cryptoUser: SHLocalCryptoUser())
    let username = "testUser"
    let password = "abc"
    
    var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    override func setUpWithError() throws {
        // Create sender on the server
        var error: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        serverProxy.createUser(name: self.username) {
            createResult in
            switch createResult {
            case .success(_):
                self.serverProxy.signIn(name: self.username) {
                    signInResult in
                    switch signInResult {
                    case .success(let authResponse):
                        do {
                            guard let authSalt = Data(base64Encoded: authResponse.encryptionProtocolSalt) else {
                                throw SHHTTPError.ServerError.unexpectedResponse(authResponse.encryptionProtocolSalt)
                            }
                            try self.user.authenticate(
                                authResponse.user,
                                bearerToken: authResponse.bearerToken,
                                encryptionProtocolSalt: authSalt,
                                ssoIdentifier: nil)
                        } catch {}
                    case .failure(let err):
                        error = err
                    }
                    group.leave()
                }
            case .failure(let err):
                error = err
                group.leave()
            }
        }
        
        let _ = group.wait(timeout: .distantFuture)
        guard error == nil else {
            XCTFail(error!.localizedDescription)
            return
        }
    }
    
    override func tearDownWithError() throws {
        try self.destroyUser()
    }
    
    private func destroyUser() throws {
        var error: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        serverProxy.deleteAccount() { result in
            if case .failure(let err) = result {
                error = err
            }
            group.leave()
        }
        
        let _ = group.wait(timeout: .distantFuture)
        guard error == nil else {
            XCTFail(error!.localizedDescription)
            return
        }
    }
    
    func testUploadAndDownload() throws {
        let plainText = "example data"
        let data = plainText.data(using: .utf8)!
        let sender = user.shUser
        let receiver = SHLocalCryptoUser()
        
        let group = DispatchGroup()
        var error: Error? = nil
        
        // Sender encrypts
        let encryptedAsset = try encrypt(data, from: sender, to: receiver)
        
        // Sender uploads
        group.enter()
        serverProxy.remoteServer.create(assets: [encryptedAsset], groupId: "groupId", filterVersions: nil) { result in
            switch result {
            case .success(let serverAssets):
                guard let serverAsset = serverAssets.first else {
                    error = SHBackgroundOperationError.fatalError("No asset created")
                    group.leave()
                    return
                }
                self.serverProxy.upload(serverAsset: serverAsset, asset: encryptedAsset) { result in
                    if case .failure(let err) = result {
                        error = err
                    }
                    group.leave()
                }
            case .failure(let err):
                error = err
                group.leave()
            }
        }
        
        let _ = group.wait(timeout: .distantFuture)
        guard error == nil else {
            XCTFail(error!.localizedDescription)
            return
        }
        
        // Receiver downloads
        group.enter()
        serverProxy.getAssets(
            withGlobalIdentifiers: [encryptedAsset.globalIdentifier],
            versions: [.lowResolution]
        ) { result in
            switch result {
            case .success(let assetsDict):
                XCTAssert(assetsDict.count == 1)
                XCTAssert(assetsDict.keys.first == encryptedAsset.globalIdentifier)
                XCTAssert(assetsDict.values.first?.globalIdentifier == encryptedAsset.globalIdentifier)
                XCTAssert(assetsDict.values.first?.encryptedVersions[.lowResolution]!.encryptedData == encryptedAsset.encryptedVersions[.lowResolution]!.encryptedData)
                XCTAssert(assetsDict.values.first?.encryptedVersions[.lowResolution]!.encryptedSecret == encryptedAsset.encryptedVersions[.lowResolution]!.encryptedSecret)
                XCTAssert(assetsDict.values.first?.encryptedVersions[.lowResolution]!.publicKeyData == encryptedAsset.encryptedVersions[.lowResolution]!.publicKeyData)
                XCTAssert(assetsDict.values.first?.encryptedVersions[.lowResolution]!.publicSignatureData == encryptedAsset.encryptedVersions[.lowResolution]!.publicSignatureData)
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let _ = group.wait(timeout: .distantFuture)
        guard error == nil else {
            XCTFail(error!.localizedDescription)
            return
        }
        
        // Receiver decrypts
        let decryptedAsset = try decrypt(encryptedAsset, receiver: receiver, sender: sender)
        let decryptedData = decryptedAsset.decryptedVersions[.lowResolution]!
        XCTAssertEqual(decryptedAsset.localIdentifier, decryptedAsset.localIdentifier)
        XCTAssertEqual(decryptedAsset.globalIdentifier, decryptedAsset.globalIdentifier)
        XCTAssertEqual(decryptedAsset.creationDate, decryptedAsset.creationDate)
        XCTAssertEqual(plainText, String(data: decryptedData, encoding: .utf8))
    }

    func encrypt(_ data: Data, from sender: SHLocalCryptoUser, to receiver: SHCryptoUser) throws -> any SHEncryptedAsset {
        let privateSecret = SymmetricKey(size: .bits256)
        let samePrivateSecret = try SymmetricKey(rawRepresentation: privateSecret.rawRepresentation)

        XCTAssertEqual(privateSecret.rawRepresentation, samePrivateSecret.rawRepresentation)
        XCTAssertEqual(privateSecret, samePrivateSecret)

        /// encrypt this asset with a new set of ephemeral symmetric keys
        let encryptedContent = try SHEncryptedData(privateSecret: samePrivateSecret, clearData: data)
        
        XCTAssertEqual(privateSecret.rawRepresentation, encryptedContent.privateSecret.rawRepresentation)
        
//        let encryptedContent = try SHEncryptedData(clearData: data)
        
        /// encrypt the secret using this user's public key so that it can be stored securely on the server
        let encryptedSecret = try SHUserContext(user: sender)
            .shareable(data: encryptedContent.privateSecret.rawRepresentation,
                       protocolSalt: kTestStaticProtocolSalt,
                       with: receiver)
        
        let encryptedVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: encryptedContent.encryptedData,
            encryptedSecret: encryptedSecret.cyphertext,
            publicKeyData: encryptedSecret.ephemeralPublicKeyData,
            publicSignatureData: encryptedSecret.signature
        )
        
        return SHGenericEncryptedAsset(
            globalIdentifier: "globalId",
            localIdentifier: "localId",
            creationDate: Date(),
            encryptedVersions: [.lowResolution: encryptedVersion]
        )
    }
    
    func decrypt(_ encryptedAsset: any SHEncryptedAsset, receiver: SHLocalCryptoUser, sender: SHCryptoUser) throws -> any SHDecryptedAsset {
        let version = encryptedAsset.encryptedVersions[.lowResolution]!
        let sharedSecret = SHShareablePayload(
            ephemeralPublicKeyData: version.publicKeyData,
            cyphertext: version.encryptedSecret,
            signature: version.publicSignatureData
        )
        let decryptedData = try SHUserContext(user: receiver)
            .decrypt(version.encryptedData,
                     usingEncryptedSecret: sharedSecret,
                     protocolSalt: kTestStaticProtocolSalt,
                     receivedFrom: sender)
        return SHGenericDecryptedAsset(
            globalIdentifier: encryptedAsset.globalIdentifier,
            localIdentifier: encryptedAsset.localIdentifier,
            decryptedVersions: [.lowResolution: decryptedData],
            creationDate: encryptedAsset.creationDate
        )
    }
}