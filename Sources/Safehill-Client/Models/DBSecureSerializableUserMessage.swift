import Foundation


class DBSecureSerializableUserMessage: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding = true
    
    enum CodingKeys: String, CodingKey {
        case interactionId
        case senderUserIdentifier
        case inReplyToAssetGlobalIdentifier
        case inReplyToInteractionId
        case encryptedMessage
        case createdAt
    }
    
    let interactionId: String
    let senderUserIdentifier: String
    let inReplyToAssetGlobalIdentifier: String?
    let inReplyToInteractionId: String?
    let encryptedMessage: String // base64EncodedData with the cipher
    let createdAt: String // ISO8601 formatted datetime
    
    init(interactionId: String,
         senderUserIdentifier: String,
         inReplyToAssetGlobalIdentifier: String?,
         inReplyToInteractionId: String?,
         encryptedMessage: String,
         createdAt: String) {
        self.interactionId = interactionId
        self.senderUserIdentifier = senderUserIdentifier
        self.inReplyToAssetGlobalIdentifier = inReplyToAssetGlobalIdentifier
        self.inReplyToInteractionId = inReplyToInteractionId
        self.encryptedMessage = encryptedMessage
        self.createdAt = createdAt
    }
    
    required convenience init?(coder decoder: NSCoder) {
        let interactionId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.interactionId.rawValue)
        let senderUserIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.senderUserIdentifier.rawValue)
        let inReplyToInteractionId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.inReplyToInteractionId.rawValue)
        let inReplyToAssetGlobalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.inReplyToAssetGlobalIdentifier.rawValue)
        let encryptedMessage = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.encryptedMessage.rawValue)
        let createdAt = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.createdAt.rawValue)
        
        guard let interactionId = interactionId as String? else {
            log.error("unexpected value for interactionId when decoding SecureSerializableUserMessage object")
            return nil
        }
        guard let senderUserIdentifier = senderUserIdentifier as String? else {
            log.error("unexpected senderUserIdentifier for name when decoding SecureSerializableUserMessage object")
            return nil
        }
        guard let encryptedMessage = encryptedMessage as String? else {
            log.error("unexpected value for encryptedMessage when decoding SecureSerializableUserMessage object")
            return nil
        }
        guard let createdAt = createdAt as String? else {
            log.error("unexpected value for createdAt when decoding SecureSerializableUserMessage object")
            return nil
        }
        
        self.init(
            interactionId: interactionId,
            senderUserIdentifier: senderUserIdentifier,
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier as String?,
            inReplyToInteractionId: inReplyToInteractionId as String?,
            encryptedMessage: encryptedMessage,
            createdAt: createdAt
        )
    }
    
    func encode(with coder: NSCoder) {
        coder.encode(interactionId, forKey: CodingKeys.interactionId.rawValue)
        coder.encode(senderUserIdentifier, forKey: CodingKeys.senderUserIdentifier.rawValue)
        coder.encode(inReplyToInteractionId, forKey: CodingKeys.inReplyToInteractionId.rawValue)
        coder.encode(inReplyToAssetGlobalIdentifier, forKey: CodingKeys.inReplyToAssetGlobalIdentifier.rawValue)
        coder.encode(encryptedMessage, forKey: CodingKeys.encryptedMessage.rawValue)
        coder.encode(createdAt, forKey: CodingKeys.createdAt.rawValue)
    }
}
