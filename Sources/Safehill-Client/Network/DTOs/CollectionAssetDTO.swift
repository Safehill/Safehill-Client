import Foundation

public struct CollectionAssetDTO: Codable {
    public let globalIdentifier: String
    public let name: String
    public let type: String
    public let uploadedAt: String // ISO string timestamp
}
