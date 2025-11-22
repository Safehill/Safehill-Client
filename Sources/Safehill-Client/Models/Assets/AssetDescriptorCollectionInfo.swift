import Foundation

/// The name of the default dropbox collection (not stored locally)
public let kSHDropboxCollectionName = "Dropbox"

public protocol SHAssetCollectionInfo {
    var collectionId: String { get }
    var collectionName: String { get }
    var visibility: String { get } // "public", "confidential", "not-shared"
    var accessType: String { get } // "granted", "accessed", "payment"
    var addedAt: String { get } // ISO8601 - when the asset was added to this collection
}

