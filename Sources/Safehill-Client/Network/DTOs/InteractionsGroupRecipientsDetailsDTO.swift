import Foundation


public struct RecipientEncryptionDetailsDTO {
    var userIdentifier: String
    var ephemeralPublicKey: String // base64EncodedData with the ephemeral public part of the key used for the encryption
    var encryptedSecret: String // base64EncodedData with the secret to decrypt the encrypted content in this group for this user
    var secretPublicSignature: String // base64EncodedData with the public signature of the user sending it
}

public struct InteractionsGroupRecipientsDetailsDTO {
    var recipients: [RecipientEncryptionDetailsDTO]
}
