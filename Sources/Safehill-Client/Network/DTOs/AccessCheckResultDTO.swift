import Foundation

public struct AccessCheckResultDTO: Codable {
    public let status: String // 'granted' | 'paywall' | 'denied' | 'loading'
    public let message: String?
    public let price: Double?
    public let visibility: String?
    public let createdBy: String?
}
