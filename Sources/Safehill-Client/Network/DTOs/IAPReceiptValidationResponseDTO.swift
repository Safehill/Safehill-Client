import Foundation

public struct IAPReceiptValidationResponseDTO: Codable {
    public let success: Bool
    public let message: String
    public let transactionId: String?

    enum CodingKeys: String, CodingKey {
        case success
        case message
        case transactionId = "transaction_id"
    }

    public init(success: Bool, message: String, transactionId: String?) {
        self.success = success
        self.message = message
        self.transactionId = transactionId
    }
}
