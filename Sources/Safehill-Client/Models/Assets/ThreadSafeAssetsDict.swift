import Foundation


actor ThreadSafeS3Errors {
    var dictionary = [String: Error]()
    
    func set(_ error: Error, forKey key: String) {
        dictionary[key] = error
    }
}


actor ThreadSafeAssetsDict {
    
    var dictionary = [String: any SHEncryptedAsset]()
    
    func add(_ encryptedAsset: any SHEncryptedAsset) {
        dictionary[encryptedAsset.globalIdentifier] = encryptedAsset
    }
}
