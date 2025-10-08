import Foundation

public struct CollectionOutputDTO: Codable {
    public let id: String
    public let name: String
    public let description: String
    public let isSystemCollection: Bool
    public let assetCount: Int
    public let visibility: String
    public let pricing: Double
    public let lastUpdated: String // ISO string timestamp
    public let createdBy: String
    public let assets: [SHServerAsset]
}
