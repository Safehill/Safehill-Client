import Foundation

public struct CheckoutSessionDTO: Codable {
    public let sessionUrl: String
    public let sessionId: String
    public let amount: Double
    public let currency: String
}
