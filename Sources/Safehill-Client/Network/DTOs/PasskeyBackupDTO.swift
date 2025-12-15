import Foundation


// MARK: - Registration DTOs (Internal wire format)

struct PasskeyRegistrationOptionsDTO: Codable {
    var challenge: String  // base64url-encoded
    var rp: RelyingPartyDTO
    var user: UserEntityDTO
    var pubKeyCredParams: [PublicKeyCredentialParametersDTO]
    var timeout: Int?
    var excludeCredentials: [PublicKeyCredentialDescriptorDTO]?
    var authenticatorSelection: AuthenticatorSelectionDTO?
    var attestation: String?  // "none", "indirect", "direct", "enterprise"

    struct RelyingPartyDTO: Codable {
        var id: String
        var name: String
    }

    struct UserEntityDTO: Codable {
        var id: String  // base64url-encoded
        var name: String
        var displayName: String
    }

    struct PublicKeyCredentialParametersDTO: Codable {
        var type: String  // "public-key"
        var alg: Int  // COSE algorithm identifier (e.g., -7 for ES256)
    }

    struct PublicKeyCredentialDescriptorDTO: Codable {
        var type: String  // "public-key"
        var id: String  // base64url-encoded credential ID
        var transports: [String]?  // ["internal", "hybrid", "usb", "nfc", "ble"]
    }

    struct AuthenticatorSelectionDTO: Codable {
        var authenticatorAttachment: String?  // "platform", "cross-platform"
        var residentKey: String?  // "discouraged", "preferred", "required"
        var requireResidentKey: Bool?
        var userVerification: String?  // "required", "preferred", "discouraged"
    }
}

struct PasskeyRegistrationCompleteDTO: Codable {
    var userIdentifier: String

    // WebAuthn credential data
    var credentialId: String
    var clientDataJSON: String
    var attestationObject: String?
    var transports: [String]?

    // Encrypted backup data
    var encryptedKeysBlob: String
    var encryptionProtocolSalt: String

    // Metadata
    var userAgent: String?
}

struct PasskeyRegistrationResponseDTO: Codable {
    var success: Bool
    var credentialId: String
    var message: String
}

// MARK: - Recovery DTOs (Internal wire format)

struct PasskeyRecoveryStartDTO: Codable {
    var userIdentifier: String
}

struct PasskeyAuthenticationOptionsDTO: Codable {
    var sessionId: String  // Session identifier for this recovery flow
    var challenge: String  // base64url-encoded
    var timeout: Int?
    var rpId: String?
    var allowCredentials: [PasskeyRegistrationOptionsDTO.PublicKeyCredentialDescriptorDTO]?
    var userVerification: String?  // "required", "preferred", "discouraged"
}

struct PasskeyRecoveryCompleteDTO: Codable {
    var sessionId: String  // Session identifier from /recover/start
    
    // WebAuthn assertion data
    var credentialId: String
    var authenticatorData: String
    var clientDataJSON: String
    var signature: String
    var userHandle: String?
}

struct PasskeyRecoveryResponseDTO: Codable {
    var success: Bool
    var encryptedKeysBlob: String
    var encryptionProtocolSalt: String
    var bearerToken: BearerToken?
    var user: SHRemoteUser?
}

// MARK: - Management DTOs (Internal wire format)

struct PasskeyCredentialInfoDTO: Codable {
    var credentialId: String
    var createdAt: Date
    var lastUsedAt: Date?
    var isActive: Bool
    var userAgent: String?
    var transports: [String]?
}

// MARK: - Conversion Extensions (Internal)

extension PasskeyRegistrationOptionsDTO {
    func toPublicModel() -> PasskeyCreationOptions {
        PasskeyCreationOptions(
            challenge: challenge,
            rpId: rp.id,
            rpName: rp.name,
            userId: user.id,
            userName: user.name,
            userDisplayName: user.displayName,
            timeout: timeout ?? 60000,
            excludedCredentialIds: excludeCredentials?.map { $0.id } ?? [],
            authenticatorAttachment: authenticatorSelection?.authenticatorAttachment,
            requireResidentKey: authenticatorSelection?.requireResidentKey ?? true,
            userVerification: authenticatorSelection?.userVerification ?? "required"
        )
    }
}

extension PasskeyRegistrationRequest {
    func toDTO() -> PasskeyRegistrationCompleteDTO {
        PasskeyRegistrationCompleteDTO(
            userIdentifier: userIdentifier,
            credentialId: credentialId,
            clientDataJSON: clientDataJSON,
            attestationObject: attestationObject,
            transports: transports,
            encryptedKeysBlob: encryptedKeysBlob,
            encryptionProtocolSalt: encryptionProtocolSalt,
            userAgent: userAgent
        )
    }
}

extension PasskeyRegistrationResponseDTO {
    func toPublicModel() -> PasskeyRegistrationResult {
        PasskeyRegistrationResult(
            success: success,
            credentialId: credentialId,
            message: message
        )
    }
}

extension PasskeyAuthenticationOptionsDTO {
    func toPublicModel() -> PasskeyAuthenticationOptions {
        PasskeyAuthenticationOptions(
            sessionId: sessionId,
            challenge: challenge,
            rpId: rpId,
            timeout: timeout ?? 60000,
            allowedCredentialIds: allowCredentials?.map { $0.id } ?? [],
            userVerification: userVerification ?? "required"
        )
    }
}

extension PasskeyRecoveryRequest {
    func toDTO() -> PasskeyRecoveryCompleteDTO {
        PasskeyRecoveryCompleteDTO(
            sessionId: sessionId,
            credentialId: credentialId,
            authenticatorData: authenticatorData,
            clientDataJSON: clientDataJSON,
            signature: signature,
            userHandle: userHandle
        )
    }
}

extension PasskeyRecoveryResponseDTO {
    func toPublicModel() -> PasskeyRecoveryResult {
        PasskeyRecoveryResult(
            success: success,
            encryptedKeysBlob: encryptedKeysBlob,
            encryptionProtocolSalt: encryptionProtocolSalt,
            bearerToken: bearerToken,
            user: user
        )
    }
}

extension PasskeyCredentialInfoDTO {
    func toPublicModel() -> PasskeyCredentialInfo {
        PasskeyCredentialInfo(
            credentialId: credentialId,
            createdAt: createdAt,
            lastUsedAt: lastUsedAt,
            isActive: isActive,
            userAgent: userAgent,
            transports: transports
        )
    }
}
