import Foundation

/// Safehill Server description of a version associated with an asset
public struct SHServerAssetVersion : Codable {
    public let versionName: String
    let publicKeyData: Data
    let publicSignatureData: Data
    let encryptedSecret: Data
    let senderPublicSignatureData: Data
    let serverPublicSignatureData: Data?
    let presignedURL: String
    let presignedURLExpiresInMinutes: Int
    let timeUploaded: String?
    
    enum CodingKeys: String, CodingKey {
        case versionName
        case publicKeyData = "ephemeralPublicKey"
        case publicSignatureData = "publicSignature"
        case encryptedSecret
        case senderPublicSignatureData = "senderPublicSignature"
        case serverPublicSignatureData = "serverPublicSignature"
        case presignedURL
        case presignedURLExpiresInMinutes
        case timeUploaded
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        versionName = try container.decode(String.self, forKey: .versionName)
        let encryptedSecretBase64 = try container.decode(String.self, forKey: .encryptedSecret)
        encryptedSecret = Data(base64Encoded: encryptedSecretBase64)!
        let publicKeyDataBase64 = try container.decode(String.self, forKey: .publicKeyData)
        publicKeyData = Data(base64Encoded: publicKeyDataBase64)!
        let publicSignatureDataBase64 = try container.decode(String.self, forKey: .publicSignatureData)
        publicSignatureData = Data(base64Encoded: publicSignatureDataBase64)!
        let senderPublicSignatureDataBase64 = try container.decode(String.self, forKey: .senderPublicSignatureData)
        senderPublicSignatureData = Data(base64Encoded: senderPublicSignatureDataBase64)!
        if let serverPublicSignatureDataBase64 = try container.decodeIfPresent(String.self, forKey: .serverPublicSignatureData) {
            serverPublicSignatureData = Data(base64Encoded: serverPublicSignatureDataBase64)
        } else {
            serverPublicSignatureData = nil
        }
        presignedURL = try container.decode(String.self, forKey: .presignedURL)
        presignedURLExpiresInMinutes = try container.decode(Int.self, forKey: .presignedURLExpiresInMinutes)
        timeUploaded = try container.decode(String.self, forKey: .timeUploaded)
    }
    
    public init(
        versionName: String,
        publicKeyData: Data,
        publicSignatureData: Data,
        encryptedSecret: Data,
        senderPublicSignatureData: Data,
        serverPublicSignatureData: Data? = nil,
        presignedURL: String,
        presignedURLExpiresInMinutes: Int,
        timeUploaded: String
    ) {
        self.versionName = versionName
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.encryptedSecret = encryptedSecret
        self.senderPublicSignatureData = senderPublicSignatureData
        self.serverPublicSignatureData = serverPublicSignatureData
        self.presignedURL = presignedURL
        self.presignedURLExpiresInMinutes = presignedURLExpiresInMinutes
        self.timeUploaded = timeUploaded
    }
}
