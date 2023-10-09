import KnowledgeBase


struct DownloadBlacklist {
    
    let kSHUsersBlacklistKey = "com.gf.safehill.user.blacklist"
    
    static var shared = DownloadBlacklist()
    
    /// Give up retrying after a download for an asset after this many attempts
    static let FailedDownloadCountThreshold = 6
    
    private let blacklistUserStorage = KBKVStore.userDefaultsStore()!
    var blacklistedUsers: [String] {
        get {
            do {
                let savedList = try self.blacklistUserStorage.value(for: kSHUsersBlacklistKey)
                if let savedList = savedList as? [String] {
                    return savedList
                }
            } catch {}
            return []
        }
        set {
            do {
                try self.blacklistUserStorage.set(value: newValue,
                                                  for: kSHUsersBlacklistKey)
            } catch {
                log.warning("unable to record kSHUserBlacklistKey status in UserDefaults KBKVStore")
            }
        }
    }
    
    var repeatedDownloadFailuresByAssetId = [String: Int]()
    
    mutating func recordFailedAttempt(globalIdentifier: String) {
        if repeatedDownloadFailuresByAssetId[globalIdentifier] == nil {
            repeatedDownloadFailuresByAssetId[globalIdentifier] = 1
        } else {
            repeatedDownloadFailuresByAssetId[globalIdentifier]! += 1
        }
    }
    
    mutating func blacklist(globalIdentifier: String) {
        repeatedDownloadFailuresByAssetId[globalIdentifier] = DownloadBlacklist.FailedDownloadCountThreshold
    }
    
    mutating func removeFromBlacklist(assetGlobalIdentifier: GlobalIdentifier) {
        repeatedDownloadFailuresByAssetId.removeValue(forKey: assetGlobalIdentifier)
    }
    
    func isBlacklisted(assetGlobalIdentifier: GlobalIdentifier) -> Bool {
        return DownloadBlacklist.FailedDownloadCountThreshold == repeatedDownloadFailuresByAssetId[assetGlobalIdentifier]
    }
    
    mutating func blacklist(userIdentifier: String) {
        var blUsers = blacklistedUsers
        guard isBlacklisted(userIdentifier: userIdentifier) == false else {
            return
        }
        
        blUsers.append(userIdentifier)
        blacklistedUsers = blUsers
    }
    
    mutating func removeFromBlacklist(userIdentifiers: [String]) {
        var blUsers = blacklistedUsers
        blUsers.removeAll(where: { userIdentifiers.contains($0) })
        blacklistedUsers = blUsers
    }
    
    mutating func removeFromBlacklistIfNotIn(userIdentifiers: [String]) {
        var blUsers = blacklistedUsers
        blUsers.removeAll(where: { userIdentifiers.contains($0) == false })
        blacklistedUsers = blUsers
    }
    
    func isBlacklisted(userIdentifier: String) -> Bool {
        blacklistedUsers.contains(userIdentifier)
    }
    
    mutating func deepClean() throws {
        let _ = try self.blacklistUserStorage.removeAll()
    }
}
