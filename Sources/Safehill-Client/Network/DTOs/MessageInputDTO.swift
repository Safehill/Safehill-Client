import Foundation

public protocol MessageInput {
    var interactionId: String? { get }
    var senderPublicIdentifier: String? { get }
    var senderPublicSignature: String? { get }
    var inReplyToAssetGlobalIdentifier: String? { get }
    var inReplyToInteractionId: String? { get }
    var encryptedMessage: String { get } // base64EncodedData with the cipher
    var createdAt: String? { get } // ISO8601 formatted datetime
}


public struct MessageInputDTO {
    public let inReplyToAssetGlobalIdentifier: String?
    public let inReplyToInteractionId: String?
    public let encryptedMessage: String // base64EncodedData with the cipher
    private let _senderPublicSignature: String  // base64EncodedData with the sender user public signature
    
    init(inReplyToAssetGlobalIdentifier: String?, inReplyToInteractionId: String?, encryptedMessage: String, senderPublicSignature: String) {
        self.inReplyToAssetGlobalIdentifier = inReplyToAssetGlobalIdentifier
        self.inReplyToInteractionId = inReplyToInteractionId
        self.encryptedMessage = encryptedMessage
        self._senderPublicSignature = senderPublicSignature
    }
}

extension MessageInputDTO: MessageInput {
    public var interactionId: String? {
        return nil
    }
    
    public var senderPublicIdentifier: String? {
        return nil
    }
    
    public var senderPublicSignature: String? {
        return self._senderPublicSignature
    }
    
    public var createdAt: String? {
        return nil
    }
}
