import Foundation
import KnowledgeBase

public struct SHGenericEncryptedAsset : SHEncryptedAsset {
    public let globalIdentifier: GlobalIdentifier
    public let localIdentifier: LocalIdentifier?
    public let creationDate: Date?
    public let encryptedVersions: [SHAssetQuality: SHEncryptedAssetVersion]
    
    public init(globalIdentifier: GlobalIdentifier,
                localIdentifier: LocalIdentifier?,
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
    public static func fromDicts(_ keyValues: KBKVPairs) throws -> [String: any SHEncryptedAsset] {
        
        var invalidKeys = Set<String>()
        
        var encryptedAssetById = [String: any SHEncryptedAsset]()
        var assetDataByGlobalIdentifierAndQuality = [String: [SHAssetQuality: Data]]()
        
        for (key, value) in keyValues {
            let keyComponents = key.components(separatedBy: "::")
            
            guard keyComponents.count == 3,
                    keyComponents.first == "data",
                  let quality = SHAssetQuality(rawValue: keyComponents[1]),
                  let filePath = value as? String
            else {
                if keyComponents.first == "data" {
                    log.error("invalid asset data key or value format for key \(key)")
                    invalidKeys.insert(key)
                    invalidKeys.insert(keyComponents[1..<keyComponents.count].joined(separator: "::"))
                }
                continue
            }
            
            let identifier = keyComponents[2]
            
            if let fileURL = URL(string: filePath) {
                guard FileManager.default.fileExists(atPath: fileURL.path) else {
                    log.error("no data for asset \(identifier) \(quality.rawValue). File at url \(filePath) does not exist")
                    invalidKeys.insert(key)
                    invalidKeys.insert(keyComponents[1..<keyComponents.count].joined(separator: "::"))
                    continue
                }
                
                do {
                    let data = try Data(contentsOf: fileURL, options: .mappedIfSafe)
                    
                    if assetDataByGlobalIdentifierAndQuality[identifier] == nil {
                        assetDataByGlobalIdentifierAndQuality[identifier] = [quality: data]
                    } else {
                        assetDataByGlobalIdentifierAndQuality[identifier]![quality] = data
                    }
                } catch {
                    log.error("no data for asset \(identifier) \(quality.rawValue). Error reading from file at \(filePath)")
                    invalidKeys.insert(key)
                    invalidKeys.insert(keyComponents[1..<keyComponents.count].joined(separator: "::"))
                }
            } else {
                log.error("invalid url for asset \(identifier): \(filePath)")
                invalidKeys.insert(key)
                invalidKeys.insert(keyComponents[1..<keyComponents.count].joined(separator: "::"))
            }
        }
        
        for (key, value) in keyValues {
            let keyComponents = key.components(separatedBy: "::")
            
            guard keyComponents.count == 2,
                  let quality = SHAssetQuality(rawValue: keyComponents[0]),
                  let rawMetadata = value as? Data
            else {
                if keyComponents.first != "data" {
                    invalidKeys.insert(key)
                    invalidKeys.insert("data::\(key)")
                    log.error("invalid asset data key or value format for key \(key)")
                }
                continue
            }
            
            guard let metadata = try? DBSecureSerializableAssetVersionMetadata.from(rawMetadata) else {
                log.error("failed to deserialize asset version metadata for key \(key)")
                invalidKeys.insert(key)
                invalidKeys.insert("data::\(key)")
                continue
            }
            
            guard let data = assetDataByGlobalIdentifierAndQuality[metadata.globalIdentifier]?[quality]
            else {
                log.warning("mismatch between asset asset version data and metadata for key \(key)")
                invalidKeys.insert(key)
                invalidKeys.insert("data::\(key)")
                continue
            }
            
            let version = SHGenericEncryptedAssetVersion(
                quality: metadata.quality,
                encryptedData: data,
                encryptedSecret: metadata.senderEncryptedSecret,
                publicKeyData: metadata.publicKey,
                publicSignatureData: metadata.publicSignature,
                verificationSignatureData: metadata.verificationSignature
            )
            
            if let existing = encryptedAssetById[metadata.globalIdentifier] {
                var versions = existing.encryptedVersions
                versions[quality] = version
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: existing.globalIdentifier,
                    localIdentifier: existing.localIdentifier,
                    creationDate: existing.creationDate,
                    encryptedVersions: versions
                )
                encryptedAssetById[metadata.globalIdentifier] = encryptedAsset
            } else {
                encryptedAssetById[metadata.globalIdentifier] = SHGenericEncryptedAsset(
                    globalIdentifier: metadata.globalIdentifier,
                    localIdentifier: metadata.localIdentifier,
                    creationDate: metadata.creationDate,
                    encryptedVersions: [quality: version]
                )
            }
        }
        
        ///
        /// Remove keys that couldn't be deserialized
        ///
        if invalidKeys.isEmpty == false,
           let assetStore = SHDBManager.sharedInstance.assetStore
        {
            do {
                let _ = try assetStore.removeValues(for: Array(invalidKeys))
            } catch {
                log.error("failed to remove invalid keys \(invalidKeys) from DB. \(error.localizedDescription)")
            }
        }
        
        return encryptedAssetById
    }
}

