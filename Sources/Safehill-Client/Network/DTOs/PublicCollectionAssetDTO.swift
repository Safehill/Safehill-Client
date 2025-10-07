import Foundation

public struct PublicCollectionAssetDTO: Codable {
    public let globalIdentifier: String
    public let name: String
    public let type: String
    public let lowResolutionVersion: SHPublicServerAssetVersion?
    public let uploadedAt: String // ISO string timestamp
}
