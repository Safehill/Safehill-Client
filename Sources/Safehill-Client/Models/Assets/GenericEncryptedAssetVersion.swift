import Foundation

public struct SHGenericEncryptedAssetVersion : SHEncryptedAssetVersion {
    public let quality: SHAssetQuality
    public let encryptedData: Data
    public let encryptedSecret: Data
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    public init(quality: SHAssetQuality,
                encryptedData: Data,
                encryptedSecret: Data,
                publicKeyData: Data,
                publicSignatureData: Data) {
        self.quality = quality
        self.encryptedData = encryptedData
        self.encryptedSecret = encryptedSecret
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
    }
    
    public static func fromDict(_ dict: [String: Any], data: Data?) -> SHEncryptedAssetVersion? {
        if let encryptedData = data,
           let qualityS = dict["quality"] as? String,
           let quality = SHAssetQuality(rawValue: qualityS),
           let encryptedSecret = dict["senderEncryptedSecret"] as? Data,
           let publicKeyData = dict["publicKey"] as? Data,
           let publicSignatureData = dict["publicSignature"] as? Data {
            return SHGenericEncryptedAssetVersion(
                quality: quality,
                encryptedData: encryptedData,
                encryptedSecret: encryptedSecret,
                publicKeyData: publicKeyData,
                publicSignatureData: publicSignatureData
            )
        }
        return nil
    }
}


