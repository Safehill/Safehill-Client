import Foundation

public struct PaymentIntentDTO: Codable {
    public let clientSecret: String
    public let amount: Double
    public let currency: String
}
