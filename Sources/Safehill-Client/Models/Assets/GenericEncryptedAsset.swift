import Foundation

public struct SHGenericEncryptedAsset : SHEncryptedAsset {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let creationDate: Date?
    public let encryptedVersions: [SHAssetQuality: SHEncryptedAssetVersion]
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                encryptedVersions: [SHAssetQuality: SHEncryptedAssetVersion]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.encryptedVersions = encryptedVersions
    }
    
    /// Deserialize key-values coming from DB into `SHEncryptedAsset` objects
    /// - Parameter keyValues: the keys and values retrieved from DB
    /// - Returns: the `SHEncryptedAsset` objects, organized by assetIdentifier
    public static func fromDicts(_ keyValues: [String: [String: Any]]) throws -> [String: any SHEncryptedAsset] {
        var encryptedAssetById = [String: any SHEncryptedAsset]()
        
        ///
        /// Snoog 1.1.3 and earlier store the data along with the metadata.
        /// More precisely, the value under key '<quality>::<assetIdentifier>' stores both the `encryptedData` and metadata associated with it.
        /// Since Snoog 1.1.4, data metadata are stored under two different keys:
        /// 1. '<quality>::<assetIdentifier>' for the metadata
        /// 2. 'data::<quality>::<assetIdentifier>' for the data
        ///
        /// The `dicts` param will then have either `N * quality` values (for 1.1.3), or `N * quality * 2` values (for 1.1.4 and later)
        ///
        /// `assetDataByGlobalIdentifierAndQuality` is version-agnostic, and retrieves the data for each asset identifier and quality.
        ///
        var assetDataByGlobalIdentifierAndQuality = [String: [SHAssetQuality: Data]]()
        for (key, value) in keyValues {
            let keyComponents = key.components(separatedBy: "::")
            var quality: SHAssetQuality? = nil
            
            if keyComponents.count == 3, keyComponents.first == "data" {
                /// Snoog 1.1.4 and later
                quality = SHAssetQuality(rawValue: keyComponents[1])
            } else if keyComponents.count == 2 {
                /// Snoog 1.1.3 and earlier
                quality = SHAssetQuality(rawValue: keyComponents[0])
            }
                
            guard let quality = quality else {
                log.critical("failed to retrieve `quality` from key value object in the local asset store. Skipping")
                continue
            }
                
            if let identifier = value["assetIdentifier"] as? String,
               let data = value["encryptedData"] as? Data {
                if assetDataByGlobalIdentifierAndQuality[identifier] == nil {
                    assetDataByGlobalIdentifierAndQuality[identifier] = [quality: data]
                } else {
                    assetDataByGlobalIdentifierAndQuality[identifier]![quality] = data
                }
            }
        }
        
        for (key, dict) in keyValues {
            let keyComponents = key.components(separatedBy: "::")
            guard keyComponents.count == 2, dict.keys.count > 2 else {
                ///
                /// Because both elements keyed by `data::<quality>::identifier` (data) and  `<quality>::identifier`
                /// (data & metadata or just metadata for Snoog >= 1.4) are present in `dicts`, we can skip elements of `dicts` that
                /// refer to the data (`data::<quality>::<identifier>` keys). Those were already retreived in the
                /// `assetDataByGlobalIdentifierAndQuality` by the logic in the for statement above.
                ///
                continue
            }
            
            guard let quality = SHAssetQuality(rawValue: keyComponents.first ?? "") else {
                log.critical("failed to retrieve `quality` from key value object in the local asset store. Skipping")
                continue
            }
            
            guard let assetIdentifier = dict["assetIdentifier"] as? String else {
                log.critical("could not deserialize local asset from dictionary=\(dict). Couldn't find assetIdentifier key")
                throw SHBackgroundOperationError.unexpectedData(dict)
            }
            
            guard let data = assetDataByGlobalIdentifierAndQuality[assetIdentifier]?[quality],
                  let version = SHGenericEncryptedAssetVersion.fromDict(dict, data: data) else {
                log.critical("could not deserialize asset version information from dictionary=\(dict)")
                throw SHBackgroundOperationError.unexpectedData(dict)
            }
            
            if let existing = encryptedAssetById[assetIdentifier] {
                var versions = existing.encryptedVersions
                versions[version.quality] = version
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: existing.globalIdentifier,
                    localIdentifier: existing.localIdentifier,
                    creationDate: existing.creationDate,
                    encryptedVersions: versions
                )
                encryptedAssetById[assetIdentifier] = encryptedAsset
            }
            else if let assetIdentifier = dict["assetIdentifier"] as? String,
                    let phAssetIdentifier = dict["applePhotosAssetIdentifier"] as? String?,
                    let creationDate = dict["creationDate"] as? Date?
            {
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: assetIdentifier,
                    localIdentifier: phAssetIdentifier,
                    creationDate: creationDate,
                    encryptedVersions: [version.quality: version]
                )
                encryptedAssetById[assetIdentifier] = encryptedAsset
            } else {
                log.critical("could not deserialize asset information from dictionary=\(dict)")
                throw SHBackgroundOperationError.unexpectedData(dict)
            }
        }
        
        return encryptedAssetById
    }
}

