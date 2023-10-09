import Foundation
import Safehill_Crypto
import KnowledgeBase
import os
import Photos

protocol SHDownloadOperationProtocol {}


public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol, SHDownloadOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    public let limit: Int?
    let user: SHLocalUser
    
    let delegates: [SHAssetDownloaderDelegate]
    let restorationDelegate: SHAssetActivityRestorationDelegate
    
    let photoIndexer: SHPhotosIndexer
    
    public init(user: SHLocalUser,
                delegates: [SHAssetDownloaderDelegate],
                restorationDelegate: SHAssetActivityRestorationDelegate,
                limitPerRun limit: Int? = nil,
                photoIndexer: SHPhotosIndexer? = nil) {
        self.user = user
        self.limit = limit
        self.delegates = delegates
        self.restorationDelegate = restorationDelegate
        self.photoIndexer = photoIndexer ?? SHPhotosIndexer()
    }
    
    var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHDownloadOperation(
            user: self.user,
            delegates: self.delegates,
            restorationDelegate: self.restorationDelegate,
            limitPerRun: self.limit,
            photoIndexer: self.photoIndexer
        )
    }
    
    private func fetchRemoteAsset(withGlobalIdentifier globalIdentifier: GlobalIdentifier,
                                  quality: SHAssetQuality,
                                  request: SHDownloadRequestQueueItem,
                                  completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void) {
        let start = CFAbsoluteTimeGetCurrent()
        
        log.info("downloading assets with identifier \(globalIdentifier) version \(quality.rawValue)")
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
                    let decryptedAsset = try SHLocalAssetStoreController(
                        user: self.user
                    ).decryptedAsset(
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
                self.log.critical("unable to download assets \(globalIdentifier) version \(quality.rawValue) from server: \(err)")
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
    
    internal func fetchApplePhotosLibraryAssets(for localIdentifiers: [String]) throws -> PHFetchResult<PHAsset>? {
        let group = DispatchGroup()
        var fetchResult: PHFetchResult<PHAsset>? = nil
        var appleLibraryFetchError: Error? = nil
        
        group.enter()
        self.photoIndexer.fetchCameraRollAssets(withFilters: [.withLocalIdentifiers(localIdentifiers)]) { result in
            switch result {
            case .success(let fullFetchResult):
                fetchResult = fullFetchResult
            case .failure(let err):
                appleLibraryFetchError = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard appleLibraryFetchError == nil else {
            throw appleLibraryFetchError!
        }
        
        return fetchResult
    }
    
    internal func fetchDescriptorsFromServer() throws -> [any SHAssetDescriptor] {
        let group = DispatchGroup()
        
        var descriptors = [any SHAssetDescriptor]()
        var error: Error? = nil
        
        group.enter()
        serverProxy.getRemoteAssetDescriptors { result in
            switch result {
            case .success(let descs):
                descriptors = descs
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        
        guard error == nil else {
            throw error!
        }
        
        return descriptors
    }
    
    ///
    /// Fetch descriptors from server.
    /// Filters blacklisted assets and users.
    /// Filter out non-completed uploads.
    /// Call the delegate with the full manifest (regardless of the limit on the task config).
    /// Limit the result based on the task config.
    /// Return the descriptors for the assets to download keyed by global identifier.
    ///
    /// - Parameter completionHandler: the callback
    internal func processDescriptors(
        completionHandler: @escaping (Swift.Result<[String: SHAssetDescriptor], Error>) -> Void
    ) {
        let start = CFAbsoluteTimeGetCurrent()
        
        var descriptors = [any SHAssetDescriptor]()
        do {
            descriptors = try self.fetchDescriptorsFromServer()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        ///
        /// Filter out the ones that were blacklisted,
        /// Also, filter out the ones that haven't been completed
        ///
        descriptors = descriptors.filter {
            DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: $0.globalIdentifier) == false
            && DownloadBlacklist.shared.isBlacklisted(userIdentifier: $0.sharingInfo.sharedByUserIdentifier) == false
            && $0.uploadState == .completed
        }
        
        guard descriptors.count > 0 else {
            completionHandler(.success([:]))
            return
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
            self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        ///
        /// Call the delegate with the full manifest of whitelisted assets
        ///
        self.delegates.forEach({
            $0.didReceiveAssetDescriptors(descriptors,
                                          referencing: users,
                                          completionHandler: nil)
        })
        
        let end = CFAbsoluteTimeGetCurrent()
        self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(descriptors.count) descriptors")
        
        var descriptorsByGlobalIdentifier = [String: any SHAssetDescriptor]()
        for descriptor in descriptors {
            descriptorsByGlobalIdentifier[descriptor.globalIdentifier] = descriptor
            ///
            /// Limit based on the task configuration
            ///
            if let limit = self.limit, descriptorsByGlobalIdentifier.count > limit {
                break
            }
        }
        completionHandler(.success(descriptorsByGlobalIdentifier))
    }
    
    
    ///
    /// Get the apple photos identifiers in the local library corresponding to the assets in the descriptors having a local identifier
    /// If they exist, then do not decrypt them but just serve the local asset.
    /// If they don't, return them in the callback method so they can be decrypted.
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors by local identifier
    ///   - completionHandler: the callback method
    internal func mergeDescriptorsWithApplePhotosAssets(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<[GlobalIdentifier: any SHAssetDescriptor], Error>) -> Void
    ) {
        let localIdentifiersInDescriptors = descriptorsByGlobalIdentifier.values.compactMap({ $0.localIdentifier })
        var phAssetsByLocalIdentifier = [String: PHAsset]()
        
        do {
            let fetchResult = try self.fetchApplePhotosLibraryAssets(for: localIdentifiersInDescriptors)
            fetchResult?.enumerateObjects { phAsset, _, _ in
                phAssetsByLocalIdentifier[phAsset.localIdentifier] = phAsset
            }
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var filteredDescriptorsByGlobalIdentifier = [String: any SHAssetDescriptor]()
        for descriptor in descriptorsByGlobalIdentifier.values {
            if let localId = descriptor.localIdentifier,
               phAssetsByLocalIdentifier.keys.contains(localId) {
                self.delegates.forEach({
                    $0.didIdentify(
                        localAsset: phAssetsByLocalIdentifier[localId]!,
                        correspondingTo: descriptor.globalIdentifier
                    )
                })
            } else {
                filteredDescriptorsByGlobalIdentifier[descriptor.globalIdentifier] = descriptor
            }
        }
        
        completionHandler(.success(filteredDescriptorsByGlobalIdentifier))
    }
    
    ///
    /// Either start download for the assets that are authorized, or request authorization for the ones that need authorization.
    /// A download needs explicit authorization from the user if the sender has never shared an asset with this user before.
    /// Once the link is established, all other downloads won't need authorization.
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors keyed by their global identifier
    /// - Parameter completionHandler: the callback method
    private func downloadOrRequestAuthorization(
        forAssetsIn descriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        var mutableDescriptors = descriptors
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
        
        self.log.info("found \(descriptors.count) assets on the server. Need to authorize \(unauthorizedDownloadDescriptors.count), can download \(authorizedDownloadDescriptors.count). limit=\(self.limit ?? 0)")
        
        let downloadsManager = SHAssetsDownloadManager(user: self.user)
        
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
            downloadsManager.waitForDownloadAuthorization(forDescriptors: unauthorizedDownloadDescriptors) { result in
                switch result {
                case .failure(let error):
                    self.log.warning("failed to enqueue unauthorized download for \(unauthorizedDownloadDescriptors.count) descriptors. \(error.localizedDescription). This operation will be attempted again")
                default: break
                }
            }
            
            var users = [SHServerUser]()
            var userIdentifiers = Set(unauthorizedDownloadDescriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
            userIdentifiers.formUnion(Set(unauthorizedDownloadDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
            
            do {
                users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
            } catch {
                self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }

            self.delegates.forEach({
                $0.didReceiveAuthorizationRequest(
                    for: unauthorizedDownloadDescriptors,
                    referencing: users
                )
            })
        }
        
        if authorizedDownloadDescriptors.count > 0 {
            ///
            /// For downloads that don't need authorization:
            /// - the delegate method `handleDownloadAuthorization(ofDescriptors:users:)` is called
            /// - descriptors are added to the unauthorized download queue
            /// - the index of assets to authorized per user is updated (userStore, keyed by `auth-<USER_ID>`)
            ///
            downloadsManager.startDownload(of: authorizedDownloadDescriptors,
                                           completionHandler: completionHandler)
        } else {
            completionHandler(.success(()))
        }
    }
    
    internal func processAssetsInDescriptors(
        descriptorsByGlobalIdentifier: [String: any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to decrypt
        ///
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        self.mergeDescriptorsWithApplePhotosAssets(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier
        ) { result in
            switch result {
            case .success(let nonLocalPhotoLibraryDescriptorsByGlobalIdentifier):
                let start = CFAbsoluteTimeGetCurrent()
                
                ///
                /// Download or request authorization for the remainder of the assets that have completed uploading
                ///
                self.downloadOrRequestAuthorization(
                    forAssetsIn: Array(nonLocalPhotoLibraryDescriptorsByGlobalIdentifier.values)
                ) { result in
                    let end = CFAbsoluteTimeGetCurrent()
                    self.log.debug("[localDownload][PERF] it took \(CFAbsoluteTime(end - start)) to decrypt \(nonLocalPhotoLibraryDescriptorsByGlobalIdentifier.count) assets in the local asset store")
                    completionHandler(result)
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
//    ///
//    /// Fetch descriptors from REMOTE server and corresponding users.
//    /// Call the delegate with the descriptors and user information.
//    /// Return the descriptors fetch for download.
//    ///
//    /// - Parameter completionHandler: the callback method
//    private func processDescriptors(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
//        var remoteDescriptors = [any SHAssetDescriptor]()
//        do { remoteDescriptors = try self.fetchDescriptorsFromServer() }
//        catch {
//            completionHandler(.failure(error))
//            return
//        }
//        
//        let existingGlobalIdentifiers = fetchResult.localDescriptors.map { $0.globalIdentifier }
//        let existingLocalIdentifiers = fetchResult.appleLibraryIdentifiers
//        
//        let start = CFAbsoluteTimeGetCurrent()
//        
//        ///
//        /// Filter out what NOT to download from the CDN:
//        /// - assets that have already been downloaded (are in `delegate.globalIdentifiersInCache`)
//        /// - assets that have a corresponding local asset (are in `delegate.localIdentifiersInCache`)
//        ///
//        
//        var globalIdentifiersToDownload = [String]()
//        var globalIdentifiersNotReadyForDownload = [String]()
//        var descriptorsByLocalIdentifier = [String: any SHAssetDescriptor]()
//        for descriptor in remoteDescriptors {
//            if let localIdentifier = descriptor.localIdentifier,
//               existingLocalIdentifiers.contains(localIdentifier) {
//                descriptorsByLocalIdentifier[localIdentifier] = descriptor
//            } else {
//                guard existingGlobalIdentifiers.contains(descriptor.globalIdentifier) == false else {
//                    continue
//                }
//                
//                if descriptor.uploadState == .completed {
//                    globalIdentifiersToDownload.append(descriptor.globalIdentifier)
//                } else {
//                    globalIdentifiersNotReadyForDownload.append(descriptor.globalIdentifier)
//                }
//            }
//        }
//                
//        ///
//        /// Fetch from server users information (`SHServerUser` objects) for all user identifiers found in all descriptors
//        ///
//        
//        var users = [SHServerUser]()
//        var userIdentifiers = Set(remoteDescriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
//        userIdentifiers.formUnion(Set(remoteDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
//        
//        do {
//            users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
//        } catch {
//            self.log.error("[sync] unable to fetch users from server: \(error.localizedDescription)")
//            completionHandler(.failure(error))
//            return
//        }
//                
//        ///
//        /// Download scenarios:
//        ///
//        /// 1. Assets on server and in the Photos library (local identifiers match) don't need to be downloaded.
//        ///     -> The delegate responsible to mark local assets "backed up" will be called
//        ///     -> If shared by "this" user, `UploadHistoryQueue` items will be created when they don't already exist.
//        ///
//        /// 2. Assets on the server not in the Photos library (local identifiers don't match), need to be downloaded.
//        ///     -> The delegate methods are responsible for adding the assets to the in-memory cache.
//        ///     -> The `SHServerProxy` is responsible to cache these in the `LocalServer`
//        ///
//        
//        if descriptorsByLocalIdentifier.count > 0 {
//            ///
//            /// Let the delegate know these local assets can be safely marked as "backed up"
//            ///
//            self.delegate.markLocalAssetsAsUploaded(descriptorsByLocalIdentifier: descriptorsByLocalIdentifier)
//            
//            ///
//            /// Update UploadHistoryQueue and ShareHistoryQueue
//            ///
//            let descriptorsByLocalIdentifierSharedByThisUser = descriptorsByLocalIdentifier.compactMapValues({ descriptor in
//                if descriptor.sharingInfo.sharedByUserIdentifier == self.user.identifier {
//                    return descriptor
//                }
//                return nil
//            })
//            if descriptorsByLocalIdentifierSharedByThisUser.count > 0 {
//                self.updateHistoryQueues(with: descriptorsByLocalIdentifierSharedByThisUser,
//                                         users: users)
//            }
//        } else {
//            self.delegate.noAssetsToDownload()
//        }
//                
//        if globalIdentifiersToDownload.count == 0 {
//            completionHandler(.success(()))
//            return
//        }
//        
//        // MARK: Enqueue the items to download
//        
//        ///
//        /// Do not download more than `limit` if a limit was set on the operation
//        ///
//        if let limit = self.limit {
//            globalIdentifiersToDownload = Array(globalIdentifiersToDownload[...min(limit, globalIdentifiersToDownload.count-1)])
//        }
//        
//        ///
//        /// Filter out the ones that were blacklisted
//        ///
//        let descriptorsForAssetsToDownload = remoteDescriptors.filter {
//            globalIdentifiersToDownload.contains($0.globalIdentifier)
//            && DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: $0.globalIdentifier) == false
//            && DownloadBlacklist.shared.isBlacklisted(userIdentifier: $0.sharingInfo.sharedByUserIdentifier) == false
//        }
//        
//        if descriptorsForAssetsToDownload.count > 0 {
//            log.debug("[sync] remote descriptors = \(remoteDescriptors.count). non-blacklisted = \(descriptorsForAssetsToDownload.count)")
//        } else {
//            completionHandler(.success(()))
//            return
//        }
//        
//
//    }
    
    private func downloadAssets(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        do {
            var count = 1
            guard let queue = try? BackgroundOperationQueue.of(type: .download) else {
                self.log.error("[downloadAssets] unable to connect to local queue or database")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
                return
            }
            
            while let item = try queue.peek() {
                let start = CFAbsoluteTimeGetCurrent()
                
                log.info("[downloadAssets] downloading assets from descriptors in item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let downloadRequest = try? content(ofQueueItem: item) as? SHDownloadRequestQueueItem else {
                    log.error("[downloadAssets] unexpected data found in DOWNLOAD queue. Dequeueing")
                    
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[downloadAssets] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                guard DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: downloadRequest.assetDescriptor.globalIdentifier) == false else {
                    self.log.info("[downloadAssets] skipping item \(downloadRequest.assetDescriptor.globalIdentifier) because it was attempted too many times")
                    
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[downloadAssets] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                let globalIdentifier = downloadRequest.assetDescriptor.globalIdentifier
                let descriptor = downloadRequest.assetDescriptor
                
                // MARK: Start
                
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegates.forEach({
                        $0.didStartDownloadOfAsset(withGlobalIdentifier: globalIdentifier, in: groupId)
                    })
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
                        
                        self.delegates.forEach({
                            $0.didFetchLowResolutionAsset(decryptedAsset)
                        })
                        for groupId in descriptor.sharingInfo.groupInfoById.keys {
                            self.delegates.forEach({
                                $0.didCompleteDownloadOfAsset(
                                    withGlobalIdentifier: decryptedAsset.globalIdentifier,
                                    in: groupId
                                )
                            })
                        }
                    case .failure(let error):
                        shouldContinue = false
                        
                        // Record the failure for the asset
                        if error is SHCypher.DecryptionError {
                            DownloadBlacklist.shared.blacklist(globalIdentifier: globalIdentifier)
                        } else {
                            DownloadBlacklist.shared.recordFailedAttempt(globalIdentifier: globalIdentifier)
                        }
                        
                        for groupId in descriptor.sharingInfo.groupInfoById.keys {
                            self.delegates.forEach({
                                $0.didFailDownloadOfAsset(
                                    withGlobalIdentifier: globalIdentifier,
                                    in: groupId,
                                    with: error
                                )
                            })
                            if DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: globalIdentifier) {
                                self.delegates.forEach({
                                    $0.didFailRepeatedlyDownloadOfAsset(
                                        withGlobalIdentifier: globalIdentifier,
                                        in: groupId
                                    )
                                })
                            }
                        }
                    }
                    group.leave()
                }
                
                let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDownloadTimeoutInMilliseconds))
                guard dispatchResult == .success, shouldContinue == true else {
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[downloadAssets] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[downloadAssets][PERF] it took \(CFAbsoluteTime(end - start)) to download the asset")
                
                do { _ = try queue.dequeue() }
                catch {
                    log.warning("[downloadAssets] asset \(globalIdentifier) was downloaded but dequeuing failed, so this operation will be attempted again.")
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("[downloadAssets] download task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
            
            completionHandler(.success(()))
            
        } catch {
            log.error("[downloadAssets] error executing download task: \(error.localizedDescription)")
            completionHandler(.failure(error))
        }
    }
    
    private func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        self.processDescriptors { descResult in
            switch descResult {
            case .failure(let error):
                self.log.error("failed to download descriptors: \(error.localizedDescription)")
                completionHandler(.failure(error))
            case .success(let descriptorsByGlobalIdentifier):
                self.processAssetsInDescriptors(
                    descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier
                ) { descAssetResult in
                    switch descAssetResult {
                    case .failure(let error):
                        self.log.error("failed to process assets in descriptors: \(error.localizedDescription)")
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
        }
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.runOnce { result in
            self.delegates.forEach({
                $0.didCompleteDownloadCycle(with: result)
            })
            self.state = .finished
        }
    }
}

// MARK: History Queue updates based on Server Descriptors

//extension SHDownloadOperation {
//    
//    ///
//    /// Based on a set of asset descriptors fetch from server, update the local history queues (`UploadHistoryQueue` and `ShareHistoryQueue`).
//    /// For instance, an asset marked as backed up on server might result as not backed up on client.
//    /// This method will ensure that the upload event on server will result in an entry in the UploadHistoryQueue,
//    /// and the share event will result in an entry in the ShareHistoryQueue.
//    ///
//    /// - Parameters:
//    ///   - descriptorsByLocalIdentifier: the list of server asset descriptors keyed by localIdentifier
//    ///   - users: the manifest of user details fetched from server
//    ///
//    private func updateHistoryQueues(with descriptorsByLocalIdentifier: [String: any SHAssetDescriptor],
//                                    users: [SHServerUser]) {
//        guard let queue = try? BackgroundOperationQueue.of(type: .successfulUpload) else {
//            self.log.error("[sync] unable to connect to local queue or database")
//            return
//        }
//        
//        for (localIdentifier, descriptor) in descriptorsByLocalIdentifier {
//            let condition = KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: localIdentifier))
//            if let keys = try? queue.keys(matching: condition),
//               keys.count > 0 {
//                ///
//                /// Nothing to do, the asset is already marked as uploaded in the queue
//                ///
//            } else {
//                ///
//                /// Determine group, event originator and shared with from `descriptor.sharingInfo.sharedByUserIdentifier`
//                ///
//                let eventOriginator = users.first(where: { $0.identifier == descriptor.sharingInfo.sharedByUserIdentifier })
//                
//                guard let eventOriginator = eventOriginator,
//                      eventOriginator.identifier == self.user.identifier else {
//                    log.warning("[sync] can't mark a local asset as backed up if not owned by this user \(self.user.name)")
//                    break
//                }
//                
//                var groupId: String? = nil
//                for (userId, gid) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
//                    if userId == eventOriginator.identifier {
//                        groupId = gid
//                        break
//                    }
//                }
//                
//                guard let groupId = groupId else {
//                    log.warning("[sync] the asset descriptor sharing information doesn't seem to include the event originator")
//                    break
//                }
//                
//                let sharedWith = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup
//                    .keys
//                    .map { userIdentifier in users.first(where: { user in user.identifier == userIdentifier } )! }
//                
//                // TODO: This is a best effort to recover state from Server, but it will still result in incorrect event dates, because of the lack of an API in KnowledgeBase.framework to enqueue an item with a specific timestamp
//                /// The timestamp should be retrieved from `descriptor.sharingInfo.groupInfoById[groupId]`
//                
////                let item = SHUploadHistoryItem(
////                    localIdentifier: localIdentifier,
////                    groupId: groupId,
////                    eventOriginator: eventOriginator,
////                    sharedWith: sharedWith
////                )
////
////                try? item.enqueue(in: UploadHistoryQueue, with: localIdentifier)
////                for delegate in self.outboundDelegates {
////                    if let delegate = delegate as? SHAssetUploaderDelegate {
////                        delegate.didCompleteUpload(
////                            itemWithLocalIdentifier: localIdentifier,
////                            globalIdentifier: descriptor.globalIdentifier,
////                            groupId: groupId
////                        )
////                    }
////                    if sharedWith.filter({ $0.identifier != self.user.identifier}).count > 0 {
////                        if let delegate = delegate as? SHAssetSharerDelegate {
////                            delegate.didCompleteSharing(
////                                itemWithLocalIdentifier: localIdentifier,
////                                globalIdentifier: descriptor.globalIdentifier,
////                                groupId: groupId,
////                                with: sharedWith
////                            )
////                        }
////                    }
////                }
//            }
//        }
//    }
//}

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
