import Foundation

// MARK: - Public API Models for Passkey Backup

/// Options for creating a new passkey credential
public struct PasskeyCreationOptions {
    public let challenge: String
    public let rpId: String
    public let rpName: String
    public let userId: String
    public let userName: String
    public let userDisplayName: String
    public let timeout: Int
    public let excludedCredentialIds: [String]
    public let authenticatorAttachment: String?
    public let requireResidentKey: Bool
    public let userVerification: String

    public init(
        challenge: String,
        rpId: String,
        rpName: String,
        userId: String,
        userName: String,
        userDisplayName: String,
        timeout: Int,
        excludedCredentialIds: [String] = [],
        authenticatorAttachment: String? = nil,
        requireResidentKey: Bool = true,
        userVerification: String = "required"
    ) {
        self.challenge = challenge
        self.rpId = rpId
        self.rpName = rpName
        self.userId = userId
        self.userName = userName
        self.userDisplayName = userDisplayName
        self.timeout = timeout
        self.excludedCredentialIds = excludedCredentialIds
        self.authenticatorAttachment = authenticatorAttachment
        self.requireResidentKey = requireResidentKey
        self.userVerification = userVerification
    }
}

/// Data required to complete passkey registration
public struct PasskeyRegistrationRequest {
    public let userIdentifier: String
    public let credentialId: String
    public let clientDataJSON: String
    public let attestationObject: String?
    public let transports: [String]?
    public let encryptedKeysBlob: String
    public let encryptionProtocolSalt: String
    public let userAgent: String?

    public init(
        userIdentifier: String,
        credentialId: String,
        clientDataJSON: String,
        attestationObject: String? = nil,
        transports: [String]? = nil,
        encryptedKeysBlob: String,
        encryptionProtocolSalt: String,
        userAgent: String? = nil
    ) {
        self.userIdentifier = userIdentifier
        self.credentialId = credentialId
        self.clientDataJSON = clientDataJSON
        self.attestationObject = attestationObject
        self.transports = transports
        self.encryptedKeysBlob = encryptedKeysBlob
        self.encryptionProtocolSalt = encryptionProtocolSalt
        self.userAgent = userAgent
    }
}

/// Result of passkey registration
public struct PasskeyRegistrationResult {
    public let success: Bool
    public let credentialId: String
    public let message: String

    public init(success: Bool, credentialId: String, message: String) {
        self.success = success
        self.credentialId = credentialId
        self.message = message
    }
}

/// Options for authenticating with a passkey
public struct PasskeyAuthenticationOptions {
    public let challenge: String
    public let rpId: String?
    public let timeout: Int
    public let allowedCredentialIds: [String]
    public let userVerification: String

    public init(
        challenge: String,
        rpId: String? = nil,
        timeout: Int,
        allowedCredentialIds: [String] = [],
        userVerification: String = "required"
    ) {
        self.challenge = challenge
        self.rpId = rpId
        self.timeout = timeout
        self.allowedCredentialIds = allowedCredentialIds
        self.userVerification = userVerification
    }
}

/// Data required to complete passkey recovery
public struct PasskeyRecoveryRequest {
    public let userIdentifier: String
    public let credentialId: String
    public let authenticatorData: String
    public let clientDataJSON: String
    public let signature: String
    public let userHandle: String?

    public init(
        userIdentifier: String,
        credentialId: String,
        authenticatorData: String,
        clientDataJSON: String,
        signature: String,
        userHandle: String? = nil
    ) {
        self.userIdentifier = userIdentifier
        self.credentialId = credentialId
        self.authenticatorData = authenticatorData
        self.clientDataJSON = clientDataJSON
        self.signature = signature
        self.userHandle = userHandle
    }
}

/// Result of passkey recovery
public struct PasskeyRecoveryResult {
    public let success: Bool
    public let encryptedKeysBlob: String
    public let encryptionProtocolSalt: String
    public let bearerToken: BearerToken?
    public let user: SHRemoteUser?

    public init(
        success: Bool,
        encryptedKeysBlob: String,
        encryptionProtocolSalt: String,
        bearerToken: BearerToken? = nil,
        user: SHRemoteUser? = nil
    ) {
        self.success = success
        self.encryptedKeysBlob = encryptedKeysBlob
        self.encryptionProtocolSalt = encryptionProtocolSalt
        self.bearerToken = bearerToken
        self.user = user
    }
}

/// Information about a registered passkey credential
public struct PasskeyCredentialInfo {
    public let credentialId: String
    public let createdAt: Date
    public let lastUsedAt: Date?
    public let isActive: Bool
    public let userAgent: String?
    public let transports: [String]?

    public init(
        credentialId: String,
        createdAt: Date,
        lastUsedAt: Date? = nil,
        isActive: Bool,
        userAgent: String? = nil,
        transports: [String]? = nil
    ) {
        self.credentialId = credentialId
        self.createdAt = createdAt
        self.lastUsedAt = lastUsedAt
        self.isActive = isActive
        self.userAgent = userAgent
        self.transports = transports
    }
}
