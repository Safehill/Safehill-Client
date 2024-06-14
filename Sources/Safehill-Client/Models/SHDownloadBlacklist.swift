import KnowledgeBase
import Foundation


public actor SHDownloadBlacklist {
    
    static let kSHUsersBlacklistKey = "com.gf.safehill.user.blacklist"
    
    public static var shared = SHDownloadBlacklist()
    
    /// Give up retrying after a download for an asset after this many attempts
    static let FailedDownloadCountThreshold = 5
    
    var repeatedDownloadFailuresByAssetId = [GlobalIdentifier: Int]()
    
    func recordFailedAttempt(globalIdentifier: GlobalIdentifier) {
        if repeatedDownloadFailuresByAssetId[globalIdentifier] == nil {
            self.repeatedDownloadFailuresByAssetId[globalIdentifier] = 1
        } else {
            self.repeatedDownloadFailuresByAssetId[globalIdentifier]! += 1
        }
    }
    
    func blacklist(globalIdentifier: GlobalIdentifier) {
        self.repeatedDownloadFailuresByAssetId[globalIdentifier] = SHDownloadBlacklist.FailedDownloadCountThreshold
    }
    
    public func removeFromBlacklist(assetGlobalIdentifier: GlobalIdentifier) {
        let _ = self.repeatedDownloadFailuresByAssetId.removeValue(forKey: assetGlobalIdentifier)
    }
    
    public func isBlacklisted(assetGlobalIdentifier: GlobalIdentifier) -> Bool {
        SHDownloadBlacklist.FailedDownloadCountThreshold == repeatedDownloadFailuresByAssetId[assetGlobalIdentifier]
    }
    
    public func areBlacklisted(assetGlobalIdentifiers: [GlobalIdentifier]) -> [GlobalIdentifier: Bool] {
        return assetGlobalIdentifiers.reduce([GlobalIdentifier: Bool]()) { partialResult, assetId in
            var result = partialResult
            result[assetId] = isBlacklisted(assetGlobalIdentifier: assetId)
            return result
        }
    }
    
    func deepClean() throws {
        repeatedDownloadFailuresByAssetId.removeAll()
    }
}
