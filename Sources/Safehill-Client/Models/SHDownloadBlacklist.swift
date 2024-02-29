import KnowledgeBase
import Foundation


public actor SHDownloadBlacklist {
    
    let kSHUsersBlacklistKey = "com.gf.safehill.user.blacklist"
    
    public static var shared = SHDownloadBlacklist()
    
    /// Give up retrying after a download for an asset after this many attempts
    static let FailedDownloadCountThreshold = 50
    
    var repeatedDownloadFailuresByAssetId = [GlobalIdentifier: Int]()
    
    private let blacklistUserStorage = KBKVStore.userDefaultsStore()!
    internal var blacklistedUsers: [UserIdentifier] {
        get {
            do {
                let savedList = try self.blacklistUserStorage.value(
                    for: kSHUsersBlacklistKey
                )
                if let savedList = savedList as? [UserIdentifier] {
                    return savedList
                }
            } catch {}
            return []
        }
        set {
            do {
                try self.blacklistUserStorage.set(
                    value: newValue,
                    for: kSHUsersBlacklistKey
                )
            } catch {
                log.warning("unable to record kSHUserBlacklistKey status in UserDefaults KBKVStore")
            }
        }
    }
    
    
    public func allBlacklistedUsers() -> [UserIdentifier] {
        return blacklistedUsers
    }
    
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
    
    public func blacklist(userIdentifier: UserIdentifier) {
        guard blacklistedUsers.contains(userIdentifier) == false else {
            return
        }
        blacklistedUsers.append(userIdentifier)
    }
    
    public func removeFromBlacklist(userIdentifiers: [UserIdentifier]) {
        self.blacklistedUsers.removeAll(where: { userIdentifiers.contains($0) })
    }
    
    func removeFromBlacklistIfNotIn(userIdentifiers: [UserIdentifier]) {
        self.blacklistedUsers.removeAll(where: { userIdentifiers.contains($0) == false })
    }
    
    public func isBlacklisted(userIdentifier: UserIdentifier) -> Bool {
        blacklistedUsers.contains(userIdentifier)
    }
    
    public func areBlacklisted(userIdentifiers: [UserIdentifier]) -> [UserIdentifier: Bool] {
        return userIdentifiers.reduce([UserIdentifier: Bool]()) { partialResult, userId in
            var result = partialResult
            result[userId] = isBlacklisted(userIdentifier: userId)
            return result
        }
    }
    
    func deepClean() throws {
        let _ = try self.blacklistUserStorage.removeAll()
        repeatedDownloadFailuresByAssetId.removeAll()
    }
}
