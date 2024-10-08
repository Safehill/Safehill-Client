import Foundation


public class EncryptionDetailsClass: NSObject, NSSecureCoding {
    public static var supportsSecureCoding: Bool = true
    
    var ephemeralPublicKey: String // base64EncodedData with the ephemeral public part of the key used for the encryption
    var encryptedSecret: String // base64EncodedData with the secret to decrypt the encrypted content in this group for this user
    var secretPublicSignature: String // base64EncodedData with the public signature used for the encryption of the secret
    var senderPublicSignature: String // base64EncodedData with the public signature of the user sending it
    
    enum CodingKeys: String, CodingKey {
        case ephemeralPublicKey
        case encryptedSecret
        case secretPublicSignature
        case senderPublicSignature
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.ephemeralPublicKey, forKey: CodingKeys.ephemeralPublicKey.rawValue)
        coder.encode(self.encryptedSecret, forKey: CodingKeys.encryptedSecret.rawValue)
        coder.encode(self.secretPublicSignature, forKey: CodingKeys.secretPublicSignature.rawValue)
        coder.encode(self.senderPublicSignature, forKey: CodingKeys.senderPublicSignature.rawValue)
    }
    
    public init(ephemeralPublicKey: String, 
                encryptedSecret: String,
                secretPublicSignature: String,
                senderPublicSignature: String) {
        self.ephemeralPublicKey = ephemeralPublicKey
        self.encryptedSecret = encryptedSecret
        self.secretPublicSignature = secretPublicSignature
        self.senderPublicSignature = senderPublicSignature
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let ephemeralPublicKey = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.ephemeralPublicKey.rawValue)
        let encryptedSecret = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.encryptedSecret.rawValue)
        let secretPublicSignature = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.secretPublicSignature.rawValue)
        let senderPublicSignature = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.senderPublicSignature.rawValue)
        
        guard let ephemeralPublicKey = ephemeralPublicKey as? String else {
            log.error("unexpected value for ephemeralPublicKey when decoding EncryptionDetailsClass object")
            return nil
        }
        guard let encryptedSecret = encryptedSecret as? String else {
            log.error("unexpected value for encryptedSecret when decoding EncryptionDetailsClass object")
            return nil
        }
        guard let secretPublicSignature = secretPublicSignature as? String else {
            log.error("unexpected value for secretPublicSignature when decoding EncryptionDetailsClass object")
            return nil
        }
        guard let senderPublicSignature = senderPublicSignature as? String else {
            log.error("unexpected value for secretPublicSignature when decoding EncryptionDetailsClass object")
            return nil
        }
        
        self.init(ephemeralPublicKey: ephemeralPublicKey, 
                  encryptedSecret: encryptedSecret,
                  secretPublicSignature: secretPublicSignature,
                  senderPublicSignature: senderPublicSignature)
    }
}


internal class RecipientEncryptionDetailsCache {
    
    ///
    /// **NOTE:** The encryption details are not stable by design.
    /// Every time a secret is encrypted with some user public key a new ephemeral key is generated, which adds security through randomization.
    ///
    /// Because the ephemeral key changes every time, we don't want to change it for every item in a group or a thread.
    /// In fact, that would overwrite these values both on the local (self details) and on the server (all user details) once every asset shared in a group.
    /// Instead, we want to retrieve the details for the group user from this cache.
    /// The cache also speeds things up as it reduces the amounts of encryptions that need to happen for the secret
    ///
    
    private var cache = NSCache<NSString, NSDictionary>()
    
    func details(for anchor: SHInteractionAnchor, anchorId: String, userIdentifier: String) -> RecipientEncryptionDetailsDTO? {
        if let cacheObj = cache.object(forKey: NSString(string: "\(anchor.rawValue)::\(anchorId)")) {
            log.debug("[RecipientEncryptionDetailsCache] cache hit \(anchor.rawValue)::\(anchorId) : \(cacheObj)")
            if let details = cacheObj[userIdentifier] as? EncryptionDetailsClass {
                log.debug("[RecipientEncryptionDetailsCache] cache hit \(anchor.rawValue)::\(anchorId)::\(userIdentifier)")
                return RecipientEncryptionDetailsDTO(
                    recipientUserIdentifier: userIdentifier,
                    ephemeralPublicKey: details.ephemeralPublicKey,
                    encryptedSecret: details.encryptedSecret,
                    secretPublicSignature: details.secretPublicSignature,
                    senderPublicSignature: details.senderPublicSignature
                )
            }
        }
        log.debug("[RecipientEncryptionDetailsCache] cache miss \(anchor.rawValue)::\(anchorId)::\(userIdentifier)")
        return nil
    }
    
    func cacheDetails(_ details: RecipientEncryptionDetailsDTO, for userIdentifier: String, in anchor: SHInteractionAnchor, anchorId: String) {
        let cacheObject = EncryptionDetailsClass(
            ephemeralPublicKey: details.ephemeralPublicKey,
            encryptedSecret: details.encryptedSecret,
            secretPublicSignature: details.secretPublicSignature,
            senderPublicSignature: details.senderPublicSignature
        )
        
        log.debug("[RecipientEncryptionDetailsCache] caching \(anchor.rawValue)::\(anchorId)::\(userIdentifier)")
        if let existing = cache.object(forKey: NSString(string: "\(anchor.rawValue)::\(anchorId)")) {
            let new = existing.mutableCopy() as! NSMutableDictionary
            new[userIdentifier] = cacheObject
            self.cache.setObject(new, forKey: NSString(string: "\(anchor.rawValue)::\(anchorId)"))
        } else {
            self.cache.setObject(NSDictionary(object: cacheObject, forKey: NSString(string: userIdentifier)),
                                 forKey: NSString(string: "\(anchor.rawValue)::\(anchorId)"))
        }
        log.debug("[RecipientEncryptionDetailsCache] new cache value \(self.cache.object(forKey: NSString(string: "\(anchor.rawValue)::\(anchorId)")) ?? [:])")
    }
    
    func evict(anchor: SHInteractionAnchor, anchorId: String) {
        self.cache.removeObject(forKey: NSString(string: "\(anchor.rawValue)::\(anchorId)"))
    }
}
