import XCTest
@testable import Safehill_Client
@testable import Safehill_Crypto
import CryptoKit

final class Safehill_ClientEncryptionUnitTests: XCTestCase {

    func testSelfSharing() throws {
        let imageData = "test".data(using: .utf8)!

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
            publicSignatureData: encryptedImageSecret.signature,
            verificationSignatureData: aPrivateSignature.publicKey.derRepresentation
        )

        let encryptedAsset = SHGenericEncryptedAsset(
            globalIdentifier: "Logo-globalId",
            localIdentifier: "Logo-localId",
            creationDate: Date(),
            encryptedVersions: [.lowResolution: encryptedVersion]
        )

        let version = encryptedAsset.encryptedVersions[SHAssetQuality.lowResolution]!
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

    func _testSharing() throws {
        let imageData = "test".data(using: .utf8)!

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
            publicSignatureData: encryptedImageSecret.signature,
            verificationSignatureData: aPrivateSignature.publicKey.derRepresentation
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
            publicSignatureData: secondEncryptedImageSecret.signature,
            verificationSignatureData: bPrivateSignature.publicKey.derRepresentation
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

final class Safehill_ClientSharingTests: XCTestCase {

    /// Test server-mediated re-encryption scenario
    /// When server re-encrypts an asset for a collection share, the verificationSignatureData
    /// contains the server's signature instead of the original sender's signature
    func testServerMediatedReEncryption() throws {
        let imageData = "test".data(using: .utf8)!

        /// Server keys (simulating server re-encryption)
        let serverPrivateKey = try P256.KeyAgreement.PrivateKey(rawRepresentation: Data(base64Encoded: "RwAv5t/YxS1KjxAJETjqD3qktKJzRqYYnOb3Zq2Gg9M=")!)
        let serverPrivateSignature = try P256.Signing.PrivateKey(rawRepresentation: Data(base64Encoded: "Uwqn8XHhV8wk2UQasF7OoyvTUe8YstnnVVz5LN+4KmA=")!)
        let serverCryptoUser = SHLocalCryptoUser(key: serverPrivateKey, signature: serverPrivateSignature)

        /// Local User B (recipient of server-mediated share)
        let bPrivateKey = try P256.KeyAgreement.PrivateKey(rawRepresentation: Data(base64Encoded: "rzp6ddhcGiJmlHvmk9JJsPa+rPiI3pKOrK+bNKLBlZg=")!)
        let bPrivateSignature = try P256.Signing.PrivateKey(rawRepresentation: Data(base64Encoded: "aRS76J3PGP+WU7+pYQCDbf18hCo5Pn5PpjGFa7erLHo=")!)
        let bLocalUser = SHLocalCryptoUser(key: bPrivateKey, signature: bPrivateSignature)
        let bRemoteUser = try SHRemoteCryptoUser(publicKeyData: bPrivateKey.publicKey.derRepresentation, publicSignatureData: bPrivateSignature.publicKey.derRepresentation)

        /// Server encrypts image for B (simulating server-mediated re-encryption)
        let encryptedImage = try SHEncryptedData(clearData: imageData)
        let encryptedImageSecret = try SHUserContext(user: serverCryptoUser)
            .shareable(data: encryptedImage.privateSecret.rawRepresentation,
                       protocolSalt: kTestStaticProtocolSalt,
                       with: bRemoteUser)

        /// The verificationSignatureData is the SERVER's signature, not the original sender's
        let encryptedVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: encryptedImage.encryptedData,
            encryptedSecret: encryptedImageSecret.cyphertext,
            publicKeyData: encryptedImageSecret.ephemeralPublicKeyData,
            publicSignatureData: encryptedImageSecret.signature,
            verificationSignatureData: serverPrivateSignature.publicKey.derRepresentation
        )

        let encryptedAsset = SHGenericEncryptedAsset(
            globalIdentifier: "ServerMediated-globalId",
            localIdentifier: "ServerMediated-localId",
            creationDate: Date(),
            encryptedVersions: [.lowResolution: encryptedVersion]
        )

        /// B decrypts using verificationSignatureData (which contains server's signature)
        let version = encryptedAsset.encryptedVersions[SHAssetQuality.lowResolution]!
        let sharedSecret = SHShareablePayload(
            ephemeralPublicKeyData: version.publicKeyData,
            cyphertext: version.encryptedSecret,
            signature: version.publicSignatureData
        )

        /// Create verifier from verificationSignatureData
        let verifier = try SHRemoteCryptoUser(
            publicKeyData: Data(),
            publicSignatureData: version.verificationSignatureData
        )

        let decryptedImage = try SHUserContext(user: bLocalUser).decrypt(
            version.encryptedData,
            usingEncryptedSecret: sharedSecret,
            protocolSalt: kTestStaticProtocolSalt,
            receivedFrom: verifier
        )

        XCTAssertEqual(imageData, decryptedImage)
    }

    /// Test that decryption fails when wrong verification signature is used
    func testDecryptionFailsWithWrongVerificationSignature() throws {
        let imageData = "test".data(using: .utf8)!

        /// User A encrypts for User B
        let aPrivateKey = try P256.KeyAgreement.PrivateKey(rawRepresentation: Data(base64Encoded: "rzp6ddhcGiJmlHvmk9JJsPa+rPiI3pKOrK+bNKLBlZg=")!)
        let aPrivateSignature = try P256.Signing.PrivateKey(rawRepresentation: Data(base64Encoded: "aRS76J3PGP+WU7+pYQCDbf18hCo5Pn5PpjGFa7erLHo=")!)
        let aLocalUser = SHLocalCryptoUser(key: aPrivateKey, signature: aPrivateSignature)

        let bPrivateKey = try P256.KeyAgreement.PrivateKey(rawRepresentation: Data(base64Encoded: "RwAv5t/YxS1KjxAJETjqD3qktKJzRqYYnOb3Zq2Gg9M=")!)
        let bPrivateSignature = try P256.Signing.PrivateKey(rawRepresentation: Data(base64Encoded: "Uwqn8XHhV8wk2UQasF7OoyvTUe8YstnnVVz5LN+4KmA=")!)
        let bLocalUser = SHLocalCryptoUser(key: bPrivateKey, signature: bPrivateSignature)
        let bRemoteUser = try SHRemoteCryptoUser(publicKeyData: bPrivateKey.publicKey.derRepresentation, publicSignatureData: bPrivateSignature.publicKey.derRepresentation)

        let encryptedImage = try SHEncryptedData(clearData: imageData)
        let encryptedImageSecret = try SHUserContext(user: aLocalUser)
            .shareable(data: encryptedImage.privateSecret.rawRepresentation,
                       protocolSalt: kTestStaticProtocolSalt,
                       with: bRemoteUser)

        /// Intentionally use WRONG verification signature (B's signature instead of A's)
        let encryptedVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: encryptedImage.encryptedData,
            encryptedSecret: encryptedImageSecret.cyphertext,
            publicKeyData: encryptedImageSecret.ephemeralPublicKeyData,
            publicSignatureData: encryptedImageSecret.signature,
            verificationSignatureData: bPrivateSignature.publicKey.derRepresentation // WRONG!
        )

        let version = encryptedVersion
        let sharedSecret = SHShareablePayload(
            ephemeralPublicKeyData: version.publicKeyData,
            cyphertext: version.encryptedSecret,
            signature: version.publicSignatureData
        )

        let wrongVerifier = try SHRemoteCryptoUser(
            publicKeyData: Data(),
            publicSignatureData: version.verificationSignatureData
        )

        /// Decryption should fail with authentication error
        do {
            let _ = try SHUserContext(user: bLocalUser).decrypt(
                version.encryptedData,
                usingEncryptedSecret: sharedSecret,
                protocolSalt: kTestStaticProtocolSalt,
                receivedFrom: wrongVerifier
            )
            XCTFail("Expected authentication error")
        } catch SHCypher.DecryptionError.authenticationError {
            // Expected
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }
}
