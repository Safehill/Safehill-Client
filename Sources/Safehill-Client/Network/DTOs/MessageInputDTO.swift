import Foundation


public struct MessageInputDTO {
    let inReplyToAssetGlobalIdentifier: String?
    let inReplyToInteractionId: String?
    let encryptedMessage: String // base64EncodedData with the cipher
    let senderPublicSignature: String  // base64EncodedData with the sender user public signature
}
