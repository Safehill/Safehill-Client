import Foundation

/// Public (non-encrypted) version of an asset in a public collection
public struct SHPublicServerAssetVersion: Codable {
    public let versionName: String
    public let timeUploaded: String?
    /// Public URL for this asset version. During asset creation, this is a presigned upload URL.
    /// After upload, this is the public access URL for downloading.
    public let publicURL: String

    public init(versionName: String, timeUploaded: String? = nil, publicURL: String) {
        self.versionName = versionName
        self.timeUploaded = timeUploaded
        self.publicURL = publicURL
    }
}
