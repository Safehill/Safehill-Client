import KnowledgeBase
import Foundation


actor DownloadBlacklist {
    
    let kSHUsersBlacklistKey = "com.gf.safehill.user.blacklist"
    
    static var shared = DownloadBlacklist()
    
    /// Give up retrying after a download for an asset after this many attempts
    static let FailedDownloadCountThreshold = 50
    
    var repeatedDownloadFailuresByAssetId = [GlobalIdentifier: Int]()
    
    private let blacklistUserStorage = KBKVStore.userDefaultsStore()!
    internal var blacklistedUsers: [String] {
        get {
            do {
                let savedList = try self.blacklistUserStorage.value(
                    for: kSHUsersBlacklistKey
                )
                if let savedList = savedList as? [String] {
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
    
    func recordFailedAttempt(globalIdentifier: GlobalIdentifier) {
        if repeatedDownloadFailuresByAssetId[globalIdentifier] == nil {
            self.repeatedDownloadFailuresByAssetId[globalIdentifier] = 1
        } else {
            self.repeatedDownloadFailuresByAssetId[globalIdentifier]! += 1
        }
    }
    
    func blacklist(globalIdentifier: GlobalIdentifier) {
        self.repeatedDownloadFailuresByAssetId[globalIdentifier] = DownloadBlacklist.FailedDownloadCountThreshold
    }
    
    func removeFromBlacklist(assetGlobalIdentifier: GlobalIdentifier) {
        let _ = self.repeatedDownloadFailuresByAssetId.removeValue(forKey: assetGlobalIdentifier)
    }
    
    func isBlacklisted(assetGlobalIdentifier: GlobalIdentifier) -> Bool {
        DownloadBlacklist.FailedDownloadCountThreshold == repeatedDownloadFailuresByAssetId[assetGlobalIdentifier]
    }
    
    func areBlacklisted(assetGlobalIdentifiers: [GlobalIdentifier]) -> [GlobalIdentifier: Bool] {
        return assetGlobalIdentifiers.reduce([GlobalIdentifier: Bool]()) { partialResult, assetId in
            var result = partialResult
            result[assetId] = isBlacklisted(assetGlobalIdentifier: assetId)
            return result
        }
    }
    
    func blacklist(userIdentifier: UserIdentifier) {
        guard blacklistedUsers.contains(userIdentifier) == false else {
            return
        }
        blacklistedUsers.append(userIdentifier)
    }
    
    func removeFromBlacklist(userIdentifiers: [UserIdentifier]) {
        self.blacklistedUsers.removeAll(where: { userIdentifiers.contains($0) })
    }
    
    func removeFromBlacklistIfNotIn(userIdentifiers: [UserIdentifier]) {
        self.blacklistedUsers.removeAll(where: { userIdentifiers.contains($0) == false })
    }
    
    func isBlacklisted(userIdentifier: UserIdentifier) -> Bool {
        blacklistedUsers.contains(userIdentifier)
    }
    
    func areBlacklisted(userIdentifiers: [UserIdentifier]) -> [UserIdentifier: Bool] {
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
