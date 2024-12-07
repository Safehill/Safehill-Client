import Foundation

internal actor ThreadSafeList<T: Codable> {
    var list = [T]()
    func append(_ item: T) {
        list.append(item)
    }
}

actor ThreadSafeDictionary<Key: Hashable, Value> {
    
    private var dictionary: [Key: Value] = [:]
    
    func getValue(forKey key: Key) -> Value? {
        return dictionary[key]
    }
    
    func setValue(_ value: Value, forKey key: Key) {
        dictionary[key] = value
    }
    
    func removeValue(forKey key: Key) {
        dictionary.removeValue(forKey: key)
    }
    
    func allKeys() -> [Key] {
        return Array(dictionary.keys)
    }
    
    func allValues() -> [Value] {
        return Array(dictionary.values)
    }
    
    func allKeyValues() -> [Key: Value] {
        return dictionary
    }
    
    func count() -> Int {
        return dictionary.count
    }
}

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
                    perceptualHash: dictionary[encryptedAsset.globalIdentifier]!.perceptualHash,
                    creationDate: dictionary[encryptedAsset.globalIdentifier]!.creationDate,
                    encryptedVersions: newEncryptedVersions
                )
            }
        }
    }
}
