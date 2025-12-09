import Foundation


// MARK: - Registration DTOs

public struct PasskeyRegistrationOptionsDTO: Codable {
    var challenge: String  // base64url-encoded
    var rp: RelyingPartyDTO
    var user: UserEntityDTO
    var pubKeyCredParams: [PublicKeyCredentialParametersDTO]
    var timeout: Int?
    var excludeCredentials: [PublicKeyCredentialDescriptorDTO]?
    var authenticatorSelection: AuthenticatorSelectionDTO?
    var attestation: String?  // "none", "indirect", "direct", "enterprise"

    public struct RelyingPartyDTO: Codable {
        var id: String
        var name: String
    }

    public struct UserEntityDTO: Codable {
        var id: String  // base64url-encoded
        var name: String
        var displayName: String
    }

    public struct PublicKeyCredentialParametersDTO: Codable {
        var type: String  // "public-key"
        var alg: Int  // COSE algorithm identifier (e.g., -7 for ES256)
    }

    public struct PublicKeyCredentialDescriptorDTO: Codable {
        var type: String  // "public-key"
        var id: String  // base64url-encoded credential ID
        var transports: [String]?  // ["internal", "hybrid", "usb", "nfc", "ble"]
    }

    public struct AuthenticatorSelectionDTO: Codable {
        var authenticatorAttachment: String?  // "platform", "cross-platform"
        var residentKey: String?  // "discouraged", "preferred", "required"
        var requireResidentKey: Bool?
        var userVerification: String?  // "required", "preferred", "discouraged"
    }
}

public struct PasskeyRegistrationCompleteDTO: Codable {
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

public struct PasskeyRegistrationResponseDTO: Codable {
    var success: Bool
    var credentialId: String
    var message: String
}

// MARK: - Recovery DTOs

public struct PasskeyRecoveryStartDTO: Codable {
    var userIdentifier: String
}

public struct PasskeyAuthenticationOptionsDTO: Codable {
    var challenge: String  // base64url-encoded
    var timeout: Int?
    var rpId: String?
    var allowCredentials: [PasskeyRegistrationOptionsDTO.PublicKeyCredentialDescriptorDTO]?
    var userVerification: String?  // "required", "preferred", "discouraged"
}

public struct PasskeyRecoveryCompleteDTO: Codable {
    var userIdentifier: String
    
    // WebAuthn assertion data
    var credentialId: String
    var authenticatorData: String
    var clientDataJSON: String
    var signature: String
    var userHandle: String?
}

public struct PasskeyRecoveryResponseDTO: Codable {
    var success: Bool
    var encryptedKeysBlob: String
    var encryptionProtocolSalt: String
    var bearerToken: BearerToken?
    var user: SHRemoteUser?
}

// MARK: - Management DTOs

public struct PasskeyCredentialInfoDTO: Codable {
    var credentialId: String
    var createdAt: Date
    var lastUsedAt: Date?
    var isActive: Bool
    var userAgent: String?
    var transports: [String]?
}
