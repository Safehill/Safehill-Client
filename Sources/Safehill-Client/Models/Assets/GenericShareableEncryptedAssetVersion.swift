import Foundation

public struct SHGenericShareableEncryptedAssetVersion : SHShareableEncryptedAssetVersion {
    public let quality: SHAssetQuality
    public let userPublicIdentifier: String
    public let encryptedSecret: Data
    public let ephemeralPublicKey: Data
    public let publicSignature: Data
    
    public init(quality: SHAssetQuality,
                userPublicIdentifier: String,
                encryptedSecret: Data,
                ephemeralPublicKey: Data,
                publicSignature: Data) {
        self.quality = quality
        self.userPublicIdentifier = userPublicIdentifier
        self.encryptedSecret = encryptedSecret
        self.ephemeralPublicKey = ephemeralPublicKey
        self.publicSignature = publicSignature
    }
}
    

