import Foundation

public struct SHServerPublicAssetVersion : Codable {
    public let versionName: String
    let publicURL: String
    let timeUploaded: String?
}
