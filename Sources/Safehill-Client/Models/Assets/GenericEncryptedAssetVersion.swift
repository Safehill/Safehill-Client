import Foundation

public struct SHGenericEncryptedAssetVersion : SHEncryptedAssetVersion {
    public let quality: SHAssetQuality
    public let encryptedData: Data
    public let encryptedSecret: Data
    public let publicKeyData: Data
    public let publicSignatureData: Data
    public let verificationSignatureData: Data

    public init(quality: SHAssetQuality,
                encryptedData: Data,
                encryptedSecret: Data,
                publicKeyData: Data,
                publicSignatureData: Data,
                verificationSignatureData: Data) {
        self.quality = quality
        self.encryptedData = encryptedData
        self.encryptedSecret = encryptedSecret
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.verificationSignatureData = verificationSignatureData
    }
}


