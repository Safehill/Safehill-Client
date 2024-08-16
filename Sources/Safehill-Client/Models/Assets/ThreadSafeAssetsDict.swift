import Foundation


actor ThreadSafeS3Errors {
    var dictionary = [String: Error]()
    
    func set(_ error: Error, forKey key: String) {
        dictionary[key] = error
    }
}


actor ThreadSafeAssetsDict {
    
    var dictionary = [GlobalIdentifier: any SHEncryptedAsset]()
    
    func add(_ encryptedAsset: any SHEncryptedAsset) {
        if dictionary[encryptedAsset.globalIdentifier] == nil {
            dictionary[encryptedAsset.globalIdentifier] = encryptedAsset
        } else {
            for encryptedVersion in encryptedAsset.encryptedVersions {
                var newEncryptedVersions = dictionary[encryptedAsset.globalIdentifier]!.encryptedVersions
                newEncryptedVersions[encryptedVersion.key] = encryptedVersion.value
                dictionary[encryptedAsset.globalIdentifier] = SHGenericEncryptedAsset(
                    globalIdentifier: dictionary[encryptedAsset.globalIdentifier]!.globalIdentifier,
                    localIdentifier: dictionary[encryptedAsset.globalIdentifier]!.localIdentifier,
                    creationDate: dictionary[encryptedAsset.globalIdentifier]!.creationDate,
                    encryptedVersions: newEncryptedVersions
                )
            }
        }
    }
}
