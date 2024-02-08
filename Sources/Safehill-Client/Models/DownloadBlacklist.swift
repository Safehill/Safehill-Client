import KnowledgeBase
import Foundation


struct DownloadBlacklist {
    
    let kSHUsersBlacklistKey = "com.gf.safehill.user.blacklist"
    
    private let readWriteQueue = DispatchQueue(label: "DownloadBlacklist.readWrite", attributes: .concurrent)
    
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
        readWriteQueue.sync(flags: .barrier) {
            if repeatedDownloadFailuresByAssetId[globalIdentifier] == nil {
                self.repeatedDownloadFailuresByAssetId[globalIdentifier] = 1
            } else {
                self.repeatedDownloadFailuresByAssetId[globalIdentifier]! += 1
            }
        }
    }
    
    mutating func blacklist(globalIdentifier: String) {
        readWriteQueue.sync(flags: .barrier) {
            self.repeatedDownloadFailuresByAssetId[globalIdentifier] = DownloadBlacklist.FailedDownloadCountThreshold
        }
    }
    
    mutating func removeFromBlacklist(assetGlobalIdentifier: GlobalIdentifier) {
        let _ = readWriteQueue.sync(flags: .barrier) {
            self.repeatedDownloadFailuresByAssetId.removeValue(forKey: assetGlobalIdentifier)
        }
    }
    
    func isBlacklisted(assetGlobalIdentifier: GlobalIdentifier) -> Bool {
        var result = false
        readWriteQueue.sync(flags: .barrier) {
            result = DownloadBlacklist.FailedDownloadCountThreshold == repeatedDownloadFailuresByAssetId[assetGlobalIdentifier]
        }
        return result
    }
    
    mutating func blacklist(userIdentifier: String) {
        readWriteQueue.sync(flags: .barrier) {
            var blUsers = blacklistedUsers
            guard blacklistedUsers.contains(userIdentifier) == false else {
                return
            }
            
            blUsers.append(userIdentifier)
            blacklistedUsers = blUsers
        }
    }
    
    mutating func removeFromBlacklist(userIdentifiers: [String]) {
        readWriteQueue.sync(flags: .barrier) {
            var blUsers = self.blacklistedUsers
            blUsers.removeAll(where: { userIdentifiers.contains($0) })
            self.blacklistedUsers = blUsers
        }
    }
    
    mutating func removeFromBlacklistIfNotIn(userIdentifiers: [String]) {
        readWriteQueue.sync(flags: .barrier) {
            var blUsers = self.blacklistedUsers
            blUsers.removeAll(where: { userIdentifiers.contains($0) == false })
            self.blacklistedUsers = blUsers
        }
    }
    
    func isBlacklisted(userIdentifier: String) -> Bool {
        var result = false
        readWriteQueue.sync(flags: .barrier) {
            result = blacklistedUsers.contains(userIdentifier)
        }
        return result
    }
    
    mutating func deepClean() throws {
        try readWriteQueue.sync(flags: .barrier) {
            let _ = try self.blacklistUserStorage.removeAll()
            repeatedDownloadFailuresByAssetId.removeAll()
        }
    }
}


// - MARK: User black/white listing

public extension SHAssetsDownloadManager {
    static var blacklistedUsers: [String] {
        DownloadBlacklist.shared.blacklistedUsers
    }
    
    static func blacklistUser(with userId: String) {
        DownloadBlacklist.shared.blacklist(userIdentifier: userId)
    }
    
    static func removeUsersFromBlacklist(with userIds: [String]) {
        DownloadBlacklist.shared.removeFromBlacklist(userIdentifiers: userIds)
    }
}
