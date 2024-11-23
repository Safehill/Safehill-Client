import SwiftUI
import Foundation

public struct SHDownloadedAsset {
    public let globalIdentifier: String
    public var localIdentifier: String?
    public var decryptedVersions: [SHAssetQuality: NSUIImage]
    public let creationDate: Date?
    
    private static func image(from data: Data) throws -> NSUIImage {
#if os(iOS)
        if let uiImage = UIImage(data: data) {
            .uiKit(uiImage)
        } else {
            throw SHBackgroundOperationError.fatalError("image from data failed")
        }
#else
        if let nsImage = NSImage(data: data) {
            .appKit(nsImage)
        } else {
            throw SHBackgroundOperationError.fatalError("image from data failed")
        }
#endif
    }
    
    public static func from(_ decryptedAsset: any SHDecryptedAsset) throws -> SHDownloadedAsset {
        SHDownloadedAsset(
            globalIdentifier: decryptedAsset.globalIdentifier,
            localIdentifier: decryptedAsset.localIdentifier,
            decryptedVersions: try decryptedAsset.decryptedVersions.mapValues({
                data in
                try Self.image(from: data)
            }),
            creationDate: decryptedAsset.creationDate
        )
    }
}
