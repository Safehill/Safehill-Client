import KnowledgeBase
import Foundation


struct DownloadBlacklist {
    
    let kSHUsersBlacklistKey = "com.gf.safehill.user.blacklist"
    
    private let writeQueue = DispatchQueue(label: "DownloadBlacklist.write", attributes: .concurrent)
    
    static var shared = DownloadBlacklist()
    
    /// Give up retrying after a download for an asset after this many attempts
    static let FailedDownloadCountThreshold = 50
    
    var repeatedDownloadFailuresByAssetId = [String: Int]()
    
    private let blacklistUserStorage = KBKVStore.userDefaultsStore()!
    fileprivate var blacklistedUsers: [String] {
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
    
    mutating func recordFailedAttempt(globalIdentifier: String) {
        writeQueue.sync(flags: .barrier) {
            if repeatedDownloadFailuresByAssetId[globalIdentifier] == nil {
                self.repeatedDownloadFailuresByAssetId[globalIdentifier] = 1
            } else {
                self.repeatedDownloadFailuresByAssetId[globalIdentifier]! += 1
            }
        }
    }
    
    mutating func blacklist(globalIdentifier: String) {
        writeQueue.sync(flags: .barrier) {
            self.repeatedDownloadFailuresByAssetId[globalIdentifier] = DownloadBlacklist.FailedDownloadCountThreshold
        }
    }
    
    mutating func removeFromBlacklist(assetGlobalIdentifier: GlobalIdentifier) {
        let _ = writeQueue.sync(flags: .barrier) {
            self.repeatedDownloadFailuresByAssetId.removeValue(forKey: assetGlobalIdentifier)
        }
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
        writeQueue.sync(flags: .barrier) {
            var blUsers = self.blacklistedUsers
            blUsers.removeAll(where: { userIdentifiers.contains($0) })
            self.blacklistedUsers = blUsers
        }
    }
    
    mutating func removeFromBlacklistIfNotIn(userIdentifiers: [String]) {
        writeQueue.sync(flags: .barrier) {
            var blUsers = self.blacklistedUsers
            blUsers.removeAll(where: { userIdentifiers.contains($0) == false })
            self.blacklistedUsers = blUsers
        }
    }
    
    func isBlacklisted(userIdentifier: String) -> Bool {
        blacklistedUsers.contains(userIdentifier)
    }
    
    mutating func deepClean() throws {
        try writeQueue.sync(flags: .barrier) {
            let _ = try self.blacklistUserStorage.removeAll()
            repeatedDownloadFailuresByAssetId.removeAll()
        }
    }
}


// - MARK: User black/white listing

public extension SHAssetsDownloadManager {
    var blacklistedUsers: [String] {
        DownloadBlacklist.shared.blacklistedUsers
    }
    
    func blacklistUser(with userId: String) {
        DownloadBlacklist.shared.blacklist(userIdentifier: userId)
    }
    
    func removeUsersFromBlacklist(with userIds: [String]) {
        DownloadBlacklist.shared.removeFromBlacklist(userIdentifiers: userIds)
    }
}
