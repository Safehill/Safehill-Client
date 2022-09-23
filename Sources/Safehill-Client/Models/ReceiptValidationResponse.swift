import Foundation

public struct SHReceiptValidationResponse: Codable {
    public let productId: String?
    public let autoRenewStatus: Int?
    public let originalTransactionId: String?
    public let isInBillingRetryPeriod: Bool?
    public let notificationType: Int?
    public let expiration: Date?
}
