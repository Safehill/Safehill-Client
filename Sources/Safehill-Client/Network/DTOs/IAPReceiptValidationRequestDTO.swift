import Foundation

public struct IAPReceiptValidationRequestDTO: Codable {
    public let jwsTransaction: String
    public let productId: String
    public let transactionId: String

    enum CodingKeys: String, CodingKey {
        case jwsTransaction = "jws_transaction"
        case productId = "product_id"
        case transactionId = "transaction_id"
    }

    public init(jwsTransaction: String, productId: String, transactionId: String) {
        self.jwsTransaction = jwsTransaction
        self.productId = productId
        self.transactionId = transactionId
    }
}
