import Foundation
import Safehill_Crypto
import KnowledgeBase
import os

struct DownloadBlacklist {
    static var shared = DownloadBlacklist()
    
    /// Give up retrying after a download for an asset after this many attempts
    static let Threshold = 6
    
    var repeatedDownloadFailuresByAssetId = [String: Int]()
    
    mutating func recordFailedAttempt(globalIdentifier: String) {
        if repeatedDownloadFailuresByAssetId[globalIdentifier] == nil {
            repeatedDownloadFailuresByAssetId[globalIdentifier] = 1
        } else {
            repeatedDownloadFailuresByAssetId[globalIdentifier]! += 1
        }
    }
    
    mutating func blacklist(globalIdentifier: String) {
        repeatedDownloadFailuresByAssetId[globalIdentifier] = DownloadBlacklist.Threshold
    }
    
    mutating func remove(globalIdentifier: String) {
        repeatedDownloadFailuresByAssetId.removeValue(forKey: globalIdentifier)
    }
    
    func isBlacklisted(globalIdentifier: String) -> Bool {
        return DownloadBlacklist.Threshold == repeatedDownloadFailuresByAssetId[globalIdentifier]
    }
}


public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    public let limit: Int?
    let user: SHLocalUser
    let delegate: SHAssetDownloaderDelegate
    let outboundDelegates: [SHOutboundAssetOperationDelegate]
    
    public init(user: SHLocalUser,
                delegate: SHAssetDownloaderDelegate,
                outboundDelegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int? = nil) {
        self.user = user
        self.limit = limit
        self.delegate = delegate
        self.outboundDelegates = outboundDelegates
    }
    
    var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHDownloadOperation(user: self.user,
                            delegate: self.delegate,
                            outboundDelegates: self.outboundDelegates,
                            limitPerRun: self.limit)
    }
    
    private func fetchRemoteAsset(withGlobalIdentifier globalIdentifier: String,
                                  quality: SHAssetQuality,
                                  request: SHDownloadRequestQueueItem,
                                  completionHandler: @escaping (Result<[String: Error], Error>) -> Void) {
        let start = CFAbsoluteTimeGetCurrent()
        
        var errorsByAssetId = [String: Error]()
        
        log.info("downloading assets with identifier \(globalIdentifier) version \(quality.rawValue)")
        serverProxy.getAssets(
            withGlobalIdentifiers: [globalIdentifier],
            versions: [quality]
        )
        { result in
            switch result {
            case .success(let assetsDict):
                if assetsDict.count > 0 {
                    for (assetId, asset) in assetsDict {
                        do {
                            let decryptedAsset = try SHLocalAssetStoreController(user: self.user).decryptedAsset(
                                encryptedAsset: asset,
                                quality: quality,
                                descriptor: request.assetDescriptor
                            )
                            
                            DownloadBlacklist.shared.remove(globalIdentifier: assetId)
                            
                            switch quality {
                            case .lowResolution:
                                self.delegate.handleLowResAsset(decryptedAsset)
                                self.delegate.completed(decryptedAsset.globalIdentifier, groupId: request.groupId)
                            case .midResolution, .hiResolution:
                                self.delegate.handleHiResAsset(decryptedAsset)
                            }
                        }
                        catch {
                            errorsByAssetId[assetId] = error
                            
                            // Record the failure for the asset
                            if error is SHCypher.DecryptionError {
                                DownloadBlacklist.shared.blacklist(globalIdentifier: assetId)
                            } else {
                                DownloadBlacklist.shared.recordFailedAttempt(globalIdentifier: assetId)
                            }
                        }
                    }
                }
                completionHandler(.success(errorsByAssetId))
            case .failure(let err):
                DownloadBlacklist.shared.blacklist(globalIdentifier: globalIdentifier)
                self.log.critical("Unable to download assets \(globalIdentifier) version \(quality.rawValue) from server: \(err)")
                completionHandler(.failure(err))
            }
            let end = CFAbsoluteTimeGetCurrent()
            self.log.debug("[PERF] \(CFAbsoluteTime(end - start)) for version \(quality.rawValue)")
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
    
    private func downloadDescriptors(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        ///
        /// Fetching assets from the ServerProxy is a 2-step process
        /// 1. Get the descriptors (no data) to determine which assets to pull. This calls the delegate with (Assets.downloading)
        /// 2. Get the low res assets data for the assets not already downloaded (based on the descriptors),
        ///
        serverProxy.getRemoteAssetDescriptors { result in
            switch result {
            case .success(let descriptors):
                
                let start = CFAbsoluteTimeGetCurrent()
                
                ///
                /// Filter out what NOT to download from the CDN:
                /// - assets that have already been downloaded (are in `delegate.globalIdentifiersInCache`)
                /// - assets that have a corresponding local asset (are in `delegate.localIdentifiersInCache`)
                ///
                
                let existingGlobalIdentifiers = self.delegate.globalIdentifiersInCache()
                let existingLocalIdentifiers = self.delegate.localIdentifiersInCache()
                
                var globalIdentifiersToDownload = [String]()
                var globalIdentifiersNotReadyForDownload = [String]()
                var descriptorsByLocalIdentifier = [String: any SHAssetDescriptor]()
                for descriptor in descriptors {
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
                var userIdentifiers = Set(descriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
                userIdentifiers.formUnion(Set(descriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
                
                do {
                    users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
                } catch {
                    self.log.error("Unable to fetch users from server: \(error.localizedDescription)")
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
                let descriptorsForAssetsToDownload = descriptors.filter {
                    globalIdentifiersToDownload.contains($0.globalIdentifier)
                    && DownloadBlacklist.shared.isBlacklisted(globalIdentifier: $0.globalIdentifier) == false
                }
                
                guard descriptorsForAssetsToDownload.count > 0 else {
                    completionHandler(.success(()))
                    return
                }
                
                self.log.info("found \(descriptorsForAssetsToDownload.count) assets on the server. Need to download \(globalIdentifiersToDownload.count). limit=\(self.limit ?? 0)")
                
                ///
                /// Call the delegate for assets that will be downloaded using Assets with empty data, created based on their descriptor
                ///
                
                self.delegate.handleAssetDescriptorResults(for: descriptorsForAssetsToDownload, users: users)

                
                ///
                /// Create items in the `DownloadQueue`, one per asset
                ///
                for newDescriptor in descriptorsForAssetsToDownload {
                    let queueItemIdentifier = newDescriptor.globalIdentifier
                    guard let existingItemIdentifiers = try? DownloadQueue.keys(matching: KBGenericCondition(.equal, value: queueItemIdentifier)),
                          existingItemIdentifiers.isEmpty else {
                        self.log.info("Not enqueuing item \(queueItemIdentifier) in the DOWNLOAD queue as a request with the same identifier hasn't been fulfilled yet")
                        continue
                    }
                    
                    let queueItem = SHDownloadRequestQueueItem(
                        assetDescriptor: newDescriptor,
                        receiverUserIdentifier: self.user.identifier
                    )
                    self.log.info("enqueuing item \(queueItemIdentifier) in the DOWNLOAD queue")
                    do {
                        try queueItem.enqueue(in: DownloadQueue, with: queueItemIdentifier)
                    } catch {
                        self.log.error("error enqueueing in the DOWNLOAD queue. \(error.localizedDescription)")
                        continue
                    }
                }
                
                let end = CFAbsoluteTimeGetCurrent()
                self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(descriptorsForAssetsToDownload.count) descriptors and enqueue download requests")
                
                completionHandler(.success(()))
                
            case .failure(let err):
                self.log.error("Unable to download descriptors from server: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
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
            if DownloadBlacklist.shared.isBlacklisted(globalIdentifier: assetId) {
                self.delegate.unrecoverableDownloadFailure(for: assetId, groupId: groupId)
            }
        }
    }
    
    private func downloadAssets(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        do {
            var count = 1
            
            while let item = try DownloadQueue.peek() {
                let start = CFAbsoluteTimeGetCurrent()
                
                log.info("downloading assets from descriptors in item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let downloadRequest = try? content(ofQueueItem: item) as? SHDownloadRequestQueueItem else {
                    log.error("unexpected data found in DOWNLOAD queue. Dequeueing")
                    
                    do { _ = try DownloadQueue.dequeue() }
                    catch {
                        log.warning("dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: nil)
                    continue
                }
                
                guard DownloadBlacklist.shared.isBlacklisted(globalIdentifier: downloadRequest.assetDescriptor.globalIdentifier) == false else {
                    self.log.info("Skipping item \(downloadRequest.assetDescriptor.globalIdentifier) because it was attempted too many times")
                    
                    do { _ = try DownloadQueue.dequeue() }
                    catch {
                        log.warning("dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: nil)
                    continue
                }
                
                let globalIdentifier = downloadRequest.assetDescriptor.globalIdentifier
                
                // MARK: Start
                
                self.delegate.didStart(globalIdentifier: globalIdentifier,
                                       groupId: downloadRequest.groupId)
                
                let group = DispatchGroup()
                var shouldContinue = true
                
                // MARK: Get Low Res asset
                
                group.enter()
                self.fetchRemoteAsset(withGlobalIdentifier: globalIdentifier,
                                      quality: .lowResolution,
                                      request: downloadRequest) { result in
                    switch result {
                        
                    case .success(let errorsByAssetId):
                        if errorsByAssetId.count > 0 {
                            self.fail(groupId: downloadRequest.groupId, errorsByAssetIdentifier: errorsByAssetId)
                        }
                        
                    case .failure(let error):
                        shouldContinue = false
                        self.fail(groupId: downloadRequest.groupId, errorsByAssetIdentifier: [globalIdentifier: error])
                    }
                    group.leave()
                }
                
                // MARK: Get mid resolution asset (asynchronously)
                
                DispatchQueue.global().async {
                    self.fetchRemoteAsset(withGlobalIdentifier: globalIdentifier,
                                          quality: .midResolution,
                                          request: downloadRequest) { _ in }
                }
                
                let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDownloadTimeoutInMilliseconds))
                guard dispatchResult == .success, shouldContinue == true else {
                    do { _ = try DownloadQueue.dequeue() }
                    catch {
                        log.warning("dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to download the asset")
                
                do { _ = try DownloadQueue.dequeue() }
                catch {
                    log.warning("asset \(globalIdentifier) was downloaded but dequeuing failed, so this operation will be attempted again.")
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("download task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
            
            completionHandler(.success(()))
            
        } catch {
            log.error("error executing download task: \(error.localizedDescription)")
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
                self.log.error("failed to download descriptors: \(error.localizedDescription)")
                completionHandler(.failure(error))
            case .success():
                ///
                /// Get all asset descriptors associated with this user from the server.
                /// Descriptors serve as a manifest to determine what to download
                ///
                self.downloadAssets { result in
                    if case .failure(let error) = result {
                        self.log.error("failed to download assets: \(error.localizedDescription)")
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
        for (localIdentifier, descriptor) in descriptorsByLocalIdentifier {
            let condition = KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: localIdentifier))
            if let keys = try? UploadHistoryQueue.keys(matching: condition),
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
                    log.warning("Can't mark a local asset as backed up if not owned by this user \(self.user.name)")
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
                    log.warning("The asset descriptor sharing information doesn't seem to include the event originator")
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
        dispatchIntervalInSeconds: 5
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
