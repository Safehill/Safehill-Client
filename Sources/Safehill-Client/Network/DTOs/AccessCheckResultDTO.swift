import Foundation

public struct AccessCheckResultDTO: Codable {
    public let status: String // 'granted' | 'paywall' | 'denied' | 'loading'
    public let message: String?
    public let price: Double?
    public let visibility: String?
    public let createdBy: String?
    
    public init(status: String, message: String?, price: Double?, visibility: String?, createdBy: String?) {
        self.status = status
        self.message = message
        self.price = price
        self.visibility = visibility
        self.createdBy = createdBy
    }
}
