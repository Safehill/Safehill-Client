import Foundation
import Safehill_Crypto
import KnowledgeBase
import os

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
                log.warning("[sync] unable to record kSHUserBlacklistKey status in UserDefaults KBKVStore")
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


public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    public let limit: Int?
    let user: SHLocalUser
    let delegate: SHAssetDownloaderDelegate
    let outboundDelegates: [SHOutboundAssetOperationDelegate]
    let photoIndexer: SHPhotosIndexer
    
    public init(user: SHLocalUser,
                delegate: SHAssetDownloaderDelegate,
                outboundDelegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int? = nil,
                photoIndexer: SHPhotosIndexer? = nil) {
        self.user = user
        self.limit = limit
        self.delegate = delegate
        self.outboundDelegates = outboundDelegates
        self.photoIndexer = photoIndexer ?? SHPhotosIndexer()
    }
    
    var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHDownloadOperation(
            user: self.user,
            delegate: self.delegate,
            outboundDelegates: self.outboundDelegates,
            limitPerRun: self.limit,
            photoIndexer: self.photoIndexer
        )
    }
    
    private func fetchRemoteAsset(withGlobalIdentifier globalIdentifier: GlobalIdentifier,
                                  quality: SHAssetQuality,
                                  request: SHDownloadRequestQueueItem,
                                  completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void) {
        let start = CFAbsoluteTimeGetCurrent()
        
        log.info("[sync] downloading assets with identifier \(globalIdentifier) version \(quality.rawValue)")
        serverProxy.getAssets(
            withGlobalIdentifiers: [globalIdentifier],
            versions: [quality]
        )
        { result in
            switch result {
            case .success(let assetsDict):
                guard assetsDict.count > 0,
                      let encryptedAsset = assetsDict[globalIdentifier] else {
                    completionHandler(.failure(SHHTTPError.ClientError.notFound))
                    return
                }
                do {
                    let decryptedAsset = try SHLocalAssetStoreController(user: self.user).decryptedAsset(
                        encryptedAsset: encryptedAsset,
                        quality: quality,
                        descriptor: request.assetDescriptor
                    )
                    completionHandler(.success(decryptedAsset))
                }
                catch {
                    completionHandler(.failure(error))
                }
            case .failure(let err):
                self.log.critical("[sync] unable to download assets \(globalIdentifier) version \(quality.rawValue) from server: \(err)")
                completionHandler(.failure(err))
            }
            let end = CFAbsoluteTimeGetCurrent()
            self.log.debug("[sync][PERF] \(CFAbsoluteTime(end - start)) for version \(quality.rawValue)")
        }
    }
    
    public func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem {
        guard let data = item.content as? Data else {
            throw KBError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let downloadRequest = unarchiver.decodeObject(of: SHDownloadRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
            throw KBError.unexpectedData(item)
        }
        
        return downloadRequest
    }
    
    internal func fetchDescriptors(skipRemote: Bool = false) throws -> (
        appleLibraryIdentifiers: [String],
        localDescriptors: [any SHAssetDescriptor],
        remoteDescriptors: [any SHAssetDescriptor]
    ) {
        let group = DispatchGroup()
        
        var appleLibraryIdentifiers = [String]()
        var localDescriptors = [any SHAssetDescriptor]()
        var remoteDescriptors = [any SHAssetDescriptor]()
        var appleLibraryFetchError: Error? = nil
        var localError: Error? = nil
        var remoteError: Error? = nil
        
        if skipRemote == false {
            group.enter()
            serverProxy.getRemoteAssetDescriptors { result in
                switch result {
                case .success(let descriptors):
                    remoteDescriptors = descriptors
                case .failure(let err):
                    remoteError = err
                }
                group.leave()
            }
        }
        
        group.enter()
        serverProxy.getLocalAssetDescriptors { result in
            switch result {
            case .success(let descriptors):
                localDescriptors = descriptors
            case .failure(let err):
                localError = err
            }
            group.leave()
        }
        
        group.enter()
        photoIndexer.fetchCameraRollAssets(withFilters: []) { result in
            switch result {
            case .success(let fullFetchResult):
                fullFetchResult?.enumerateObjects { phAsset, count, stop in
                    appleLibraryIdentifiers.append(phAsset.localIdentifier)
                }
            case .failure(let err):
                appleLibraryFetchError = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        
        guard localError == nil else {
            throw localError!
        }
        guard remoteError == nil else {
            throw remoteError!
        }
        guard appleLibraryFetchError == nil else {
            throw appleLibraryFetchError!
        }
        
        return (
            appleLibraryIdentifiers: appleLibraryIdentifiers,
            localDescriptors: localDescriptors,
            remoteDescriptors: remoteDescriptors
        )
    }
    
    private func downloadDescriptors(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        ///
        /// Fetching assets from the ServerProxy is a 2-step process
        /// 1. Get the remote descriptors (no data) to determine which assets to pull based on the local descriptor (and the local Apple Photos library, to avoid duplicates; for these we'll mark Photos library assets  as "uploaded").
        /// 2. Get the low res assets data for the assets not already downloaded (based on the descriptors),
        ///
        
        let fetchResult: (
            appleLibraryIdentifiers: [String],
            localDescriptors: [any SHAssetDescriptor],
            remoteDescriptors: [any SHAssetDescriptor]
        )
        do { fetchResult = try self.fetchDescriptors() }
        catch {
            completionHandler(.failure(error))
            return
        }
        
        let remoteDescriptors = fetchResult.remoteDescriptors
        let existingGlobalIdentifiers = fetchResult.localDescriptors.map { $0.globalIdentifier }
        let existingLocalIdentifiers = fetchResult.appleLibraryIdentifiers
        
        let start = CFAbsoluteTimeGetCurrent()
        
        ///
        /// Filter out what NOT to download from the CDN:
        /// - assets that have already been downloaded (are in `delegate.globalIdentifiersInCache`)
        /// - assets that have a corresponding local asset (are in `delegate.localIdentifiersInCache`)
        ///
                
        var globalIdentifiersToDownload = [String]()
        var globalIdentifiersNotReadyForDownload = [String]()
        var descriptorsByLocalIdentifier = [String: any SHAssetDescriptor]()
        for descriptor in remoteDescriptors {
            if let localIdentifier = descriptor.localIdentifier,
               existingLocalIdentifiers.contains(localIdentifier) {
                descriptorsByLocalIdentifier[localIdentifier] = descriptor
            } else {
                guard existingGlobalIdentifiers.contains(descriptor.globalIdentifier) == false else {
                    continue
                }
                
                if descriptor.uploadState == .completed {
                    globalIdentifiersToDownload.append(descriptor.globalIdentifier)
                } else {
                    globalIdentifiersNotReadyForDownload.append(descriptor.globalIdentifier)
                }
            }
        }
                
        ///
        /// Fetch from server users information (`SHServerUser` objects) for all user identifiers found in all descriptors
        ///
        
        var users = [SHServerUser]()
        var userIdentifiers = Set(remoteDescriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
        userIdentifiers.formUnion(Set(remoteDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
        
        do {
            users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
        } catch {
            self.log.error("[sync] unable to fetch users from server: \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
                
        ///
        /// Download scenarios:
        ///
        /// 1. Assets on server and in the Photos library (local identifiers match) don't need to be downloaded.
        ///     -> The delegate responsible to mark local assets "backed up" will be called
        ///     -> If shared by "this" user, `UploadHistoryQueue` items will be created when they don't already exist.
        ///
        /// 2. Assets on the server not in the Photos library (local identifiers don't match), need to be downloaded.
        ///     -> The delegate methods are responsible for adding the assets to the in-memory cache.
        ///     -> The `SHServerProxy` is responsible to cache these in the `LocalServer`
        ///
        
        if descriptorsByLocalIdentifier.count > 0 {
            ///
            /// Let the delegate know these local assets can be safely marked as "backed up"
            ///
            self.delegate.markLocalAssetsAsUploaded(descriptorsByLocalIdentifier: descriptorsByLocalIdentifier)
            
            ///
            /// Update UploadHistoryQueue and ShareHistoryQueue
            ///
            let descriptorsByLocalIdentifierSharedByThisUser = descriptorsByLocalIdentifier.compactMapValues({ descriptor in
                if descriptor.sharingInfo.sharedByUserIdentifier == self.user.identifier {
                    return descriptor
                }
                return nil
            })
            if descriptorsByLocalIdentifierSharedByThisUser.count > 0 {
                self.updateHistoryQueues(with: descriptorsByLocalIdentifierSharedByThisUser,
                                         users: users)
            }
        } else {
            self.delegate.noAssetsToDownload()
        }
                
        if globalIdentifiersToDownload.count == 0 {
            completionHandler(.success(()))
            return
        }
        
        // MARK: Enqueue the items to download
        
        ///
        /// Do not download more than `limit` if a limit was set on the operation
        ///
        if let limit = self.limit {
            globalIdentifiersToDownload = Array(globalIdentifiersToDownload[...min(limit, globalIdentifiersToDownload.count-1)])
        }
        
        ///
        /// Filter out the ones that were blacklisted
        ///
        let descriptorsForAssetsToDownload = remoteDescriptors.filter {
            globalIdentifiersToDownload.contains($0.globalIdentifier)
            && DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: $0.globalIdentifier) == false
            && DownloadBlacklist.shared.isBlacklisted(userIdentifier: $0.sharingInfo.sharedByUserIdentifier) == false
        }
        
        if descriptorsForAssetsToDownload.count > 0 {
            log.debug("[sync] remote descriptors = \(remoteDescriptors.count). non-blacklisted = \(descriptorsForAssetsToDownload.count)")
        } else {
            completionHandler(.success(()))
            return
        }
        
        ///
        /// Figure out which ones need authorization.
        /// A download needs explicit authorization from the user if the sender has never shared an asset with this user before.
        /// Once the link is established, all other downloads won't need authorization.
        ///
        
        var mutableDescriptors = descriptorsForAssetsToDownload
        let partitionIndex = mutableDescriptors.partition { descr in
            if descr.sharingInfo.sharedByUserIdentifier == self.user.identifier {
                return true
            }
            do {
                return try SHKGQuery.isKnownUser(withIdentifier: descr.sharingInfo.sharedByUserIdentifier)
            } catch {
                return false
            }
        }
        let unauthorizedDownloadDescriptors = Array(mutableDescriptors[..<partitionIndex])
        let authorizedDownloadDescriptors = Array(mutableDescriptors[partitionIndex...])
        
        self.log.info("[sync] found \(descriptorsForAssetsToDownload.count) assets on the server. Need to authorize \(unauthorizedDownloadDescriptors.count), can download \(authorizedDownloadDescriptors.count). limit=\(self.limit ?? 0)")
        
        let downloadController = SHAssetDownloadController(user: self.user, delegate: self.delegate)
        
        if unauthorizedDownloadDescriptors.count > 0 {
            ///
            /// For downloads waiting explicit authorization:
            /// - descriptors are added to the unauthorized download queue
            /// - the index of assets to authorized per user is updated (userStore, keyed by `auth-<USER_ID>`)
            /// - the delegate method `handleDownloadAuthorization(ofDescriptors:users:)` is called
            ///
            /// When the authorization comes (via `SHAssetDownloadController::authorizeDownloads(for:completionHandler:)`):
            /// - the downloads will move from the unauthorized to the authorized queue
            /// - the delegate method `handleAssetDescriptorResults(for:user:)` is called
            ///
            downloadController.waitForDownloadAuthorization(forDescriptors: unauthorizedDownloadDescriptors) { result in
                switch result {
                case .failure(let error):
                    self.log.warning("[sync] failed to enqueue unauthorized download for \(remoteDescriptors.count) descriptors. \(error.localizedDescription). This operation will be attempted again")
                default: break
                }
            }

            self.delegate.handleDownloadAuthorization(ofDescriptors: unauthorizedDownloadDescriptors, users: users)
        }
        
        if authorizedDownloadDescriptors.count > 0 {
            ///
            /// For downloads that don't need authorization:
            /// - the delegate method `handleDownloadAuthorization(ofDescriptors:users:)` is called
            /// - descriptors are added to the unauthorized download queue
            /// - the index of assets to authorized per user is updated (userStore, keyed by `auth-<USER_ID>`)
            ///
            downloadController.startDownloadOf(descriptors: authorizedDownloadDescriptors, from: users) { result in
                switch result {
                case .failure(let error):
                    completionHandler(.failure(error))
                case .success():
                    let end = CFAbsoluteTimeGetCurrent()
                    self.log.debug("[sync][PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(descriptorsForAssetsToDownload.count) descriptors and enqueue download requests")
                    completionHandler(.success(()))
                }
            }
        } else {
            completionHandler(.success(()))
        }
    }
    
    private func fail(groupId: String,
                      errorsByAssetIdentifier: [String: Error]) {
        guard errorsByAssetIdentifier.count > 0 else {
            return
        }
        
        self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: errorsByAssetIdentifier)
        
        for (assetId, _) in errorsByAssetIdentifier {
            // Call the delegate if the failure has occurred enough times
            if DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: assetId) {
                self.delegate.unrecoverableDownloadFailure(for: assetId, groupId: groupId)
            }
        }
    }
    
    private func downloadAssets(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        do {
            var count = 1
            guard let queue = try? BackgroundOperationQueue.of(type: .download) else {
                self.log.error("[sync] unable to connect to local queue or database")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
                return
            }
            
            while let item = try queue.peek() {
                let start = CFAbsoluteTimeGetCurrent()
                
                log.info("[sync] downloading assets from descriptors in item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let downloadRequest = try? content(ofQueueItem: item) as? SHDownloadRequestQueueItem else {
                    log.error("[sync] unexpected data found in DOWNLOAD queue. Dequeueing")
                    
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[sync] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: nil)
                    continue
                }
                
                guard DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: downloadRequest.assetDescriptor.globalIdentifier) == false else {
                    self.log.info("[sync] skipping item \(downloadRequest.assetDescriptor.globalIdentifier) because it was attempted too many times")
                    
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[sync] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: nil)
                    continue
                }
                
                let globalIdentifier = downloadRequest.assetDescriptor.globalIdentifier
                let descriptor = downloadRequest.assetDescriptor
                
                // MARK: Start
                
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegate.didStart(globalIdentifier: globalIdentifier,
                                           groupId: groupId)
                }
                
                let group = DispatchGroup()
                var shouldContinue = true
                
                // MARK: Get Low Res asset
                
                group.enter()
                self.fetchRemoteAsset(withGlobalIdentifier: globalIdentifier,
                                      quality: .lowResolution,
                                      request: downloadRequest) { result in
                    switch result {
                    case .success(let decryptedAsset):
                        DownloadBlacklist.shared.removeFromBlacklist(assetGlobalIdentifier: globalIdentifier)
                        
                        self.delegate.handleLowResAsset(decryptedAsset)
                        for groupId in descriptor.sharingInfo.groupInfoById.keys {
                            self.delegate.completed(decryptedAsset.globalIdentifier, groupId: groupId)
                        }
                    case .failure(let error):
                        shouldContinue = false
                        for groupId in descriptor.sharingInfo.groupInfoById.keys {
                            self.fail(groupId: groupId, errorsByAssetIdentifier: [globalIdentifier: error])
                        }
                        
                        // Record the failure for the asset
                        if error is SHCypher.DecryptionError {
                            DownloadBlacklist.shared.blacklist(globalIdentifier: globalIdentifier)
                        } else {
                            DownloadBlacklist.shared.recordFailedAttempt(globalIdentifier: globalIdentifier)
                        }
                        
                        for groupId in descriptor.sharingInfo.groupInfoById.keys {
                            self.delegate.failed(globalIdentifier, groupId: groupId)
                        }
                    }
                    group.leave()
                }
                
                let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDownloadTimeoutInMilliseconds))
                guard dispatchResult == .success, shouldContinue == true else {
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[sync] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[sync][PERF] it took \(CFAbsoluteTime(end - start)) to download the asset")
                
                do { _ = try queue.dequeue() }
                catch {
                    log.warning("[sync] asset \(globalIdentifier) was downloaded but dequeuing failed, so this operation will be attempted again.")
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("[sync] download task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
            
            completionHandler(.success(()))
            
        } catch {
            log.error("[sync] error executing download task: \(error.localizedDescription)")
            completionHandler(.failure(error))
        }
    }
    
    private func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        self.downloadDescriptors { result in
            switch result {
            case .failure(let error):
                self.log.error("[sync] failed to download descriptors: \(error.localizedDescription)")
                completionHandler(.failure(error))
            case .success():
                ///
                /// Get all asset descriptors associated with this user from the server.
                /// Descriptors serve as a manifest to determine what to download
                ///
                self.downloadAssets { result in
                    if case .failure(let error) = result {
                        self.log.error("[sync] failed to download assets: \(error.localizedDescription)")
                        completionHandler(.failure(error))
                    } else {
                        completionHandler(.success(()))
                    }
                }
            }
        }
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.runOnce { result in
            self.delegate.downloadOperationFinished(result)
            self.state = .finished
        }
    }
}

// MARK: History Queue updates based on Server Descriptors

extension SHDownloadOperation {
    
    ///
    /// Based on a set of asset descriptors fetch from server, update the local history queues (`UploadHistoryQueue` and `ShareHistoryQueue`).
    /// For instance, an asset marked as backed up on server might result as not backed up on client.
    /// This method will ensure that the upload event on server will result in an entry in the UploadHistoryQueue,
    /// and the share event will result in an entry in the ShareHistoryQueue.
    ///
    /// - Parameters:
    ///   - descriptorsByLocalIdentifier: the list of server asset descriptors keyed by localIdentifier
    ///   - users: the manifest of user details fetched from server
    ///
    private func updateHistoryQueues(with descriptorsByLocalIdentifier: [String: any SHAssetDescriptor],
                                    users: [SHServerUser]) {
        guard let queue = try? BackgroundOperationQueue.of(type: .successfulUpload) else {
            self.log.error("[sync] unable to connect to local queue or database")
            return
        }
        
        for (localIdentifier, descriptor) in descriptorsByLocalIdentifier {
            let condition = KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: localIdentifier))
            if let keys = try? queue.keys(matching: condition),
               keys.count > 0 {
                ///
                /// Nothing to do, the asset is already marked as uploaded in the queue
                ///
            } else {
                ///
                /// Determine group, event originator and shared with from `descriptor.sharingInfo.sharedByUserIdentifier`
                ///
                let eventOriginator = users.first(where: { $0.identifier == descriptor.sharingInfo.sharedByUserIdentifier })
                
                guard let eventOriginator = eventOriginator,
                      eventOriginator.identifier == self.user.identifier else {
                    log.warning("[sync] can't mark a local asset as backed up if not owned by this user \(self.user.name)")
                    break
                }
                
                var groupId: String? = nil
                for (userId, gid) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                    if userId == eventOriginator.identifier {
                        groupId = gid
                        break
                    }
                }
                
                guard let groupId = groupId else {
                    log.warning("[sync] the asset descriptor sharing information doesn't seem to include the event originator")
                    break
                }
                
                let sharedWith = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup
                    .keys
                    .map { userIdentifier in users.first(where: { user in user.identifier == userIdentifier } )! }
                
                // TODO: This is a best effort to recover state from Server, but it will still result in incorrect event dates, because of the lack of an API in KnowledgeBase.framework to enqueue an item with a specific timestamp
                /// The timestamp should be retrieved from `descriptor.sharingInfo.groupInfoById[groupId]`
                
//                let item = SHUploadHistoryItem(
//                    localIdentifier: localIdentifier,
//                    groupId: groupId,
//                    eventOriginator: eventOriginator,
//                    sharedWith: sharedWith
//                )
//
//                try? item.enqueue(in: UploadHistoryQueue, with: localIdentifier)
//                for delegate in self.outboundDelegates {
//                    if let delegate = delegate as? SHAssetUploaderDelegate {
//                        delegate.didCompleteUpload(
//                            itemWithLocalIdentifier: localIdentifier,
//                            globalIdentifier: descriptor.globalIdentifier,
//                            groupId: groupId
//                        )
//                    }
//                    if sharedWith.filter({ $0.identifier != self.user.identifier}).count > 0 {
//                        if let delegate = delegate as? SHAssetSharerDelegate {
//                            delegate.didCompleteSharing(
//                                itemWithLocalIdentifier: localIdentifier,
//                                globalIdentifier: descriptor.globalIdentifier,
//                                groupId: groupId,
//                                with: sharedWith
//                            )
//                        }
//                    }
//                }
            }
        }
    }
}

// MARK: - Download Operation Processor

public class SHAssetsDownloadQueueProcessor : SHBackgroundOperationProcessor<SHDownloadOperation> {
    
    public static var shared = SHAssetsDownloadQueueProcessor(
        delayedStartInSeconds: 0,
        dispatchIntervalInSeconds: 7
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
