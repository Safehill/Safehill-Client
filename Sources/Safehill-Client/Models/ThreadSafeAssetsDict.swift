import Foundation


final class ThreadSafeS3Errors {
    private var queue = DispatchQueue(label: "com.gf.safehill.Snoog.ThreadSafeS3Errors", attributes: .concurrent)
    
    private var _dictionary = [String: Error]()
    
    func set(_ error: Error, forKey key: String) {
        queue.sync(flags: .barrier) {
            _dictionary[key] = error
        }
    }
    
    func toDict() -> [String: Error] {
        return _dictionary
    }
}


final class ThreadSafeAssetsDict {
    
    private var queue = DispatchQueue(label: "com.gf.safehill.Snoog.ThreadSafeAssetsDict", attributes: .concurrent)
    
    private var _dictionary = [String: any SHEncryptedAsset]()
    var dictionary: [String: any SHEncryptedAsset] {
        queue.sync {
            _dictionary
        }
    }
    
    func add(_ encryptedAsset: any SHEncryptedAsset) {
        queue.sync(flags: .barrier) {
            _dictionary[encryptedAsset.globalIdentifier] = encryptedAsset
        }
    }
    
    func toDict() -> [String: any SHEncryptedAsset] {
        return _dictionary
    }
}
