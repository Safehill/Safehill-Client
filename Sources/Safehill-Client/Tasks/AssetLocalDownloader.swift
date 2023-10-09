import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


public class SHLocalDownloadOperation: SHDownloadOperation {
    
    let restorationDelegate: SHAssetLocalDownloaderDelegate
    
    @available(*, unavailable)
    public override init(user: SHLocalUser,
                         delegates: [SHAssetDownloaderDelegate],
                         limitPerRun limit: Int? = nil,
                         photoIndexer: SHPhotosIndexer? = nil) {
        fatalError("Not supported")
    }
    
    public init(user: SHLocalUser,
                delegates: [SHAssetDownloaderDelegate],
                restorationDelegate: SHAssetLocalDownloaderDelegate) {
        self.restorationDelegate = restorationDelegate
        super.init(user: user, delegates: delegates)
    }
    
    internal override func fetchDescriptorsFromServer() throws -> [any SHAssetDescriptor] {
        let group = DispatchGroup()
        
        var descriptors = [any SHAssetDescriptor]()
        var error: Error? = nil
        
        group.enter()
        serverProxy.getLocalAssetDescriptors { result in
            switch result {
            case .success(let descs):
                descriptors = descs
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        
        guard error == nil else {
            throw error!
        }
        
        return descriptors
    }
    
    internal func decryptFromLocalStore(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        let localAssetsStore = SHLocalAssetStoreController(user: self.user)
        guard let encryptedAssets = try? localAssetsStore.encryptedAssets(
            with: Array(descriptorsByGlobalIdentifier.keys),
            versions: [.lowResolution],
            cacheHiResolution: false
        ) else {
            self.log.error("unable to fetch local assets")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("unable to fetch local assets")))
            return
        }
        
        let descriptors = Array(descriptorsByGlobalIdentifier.values)
        
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
        
        for (globalAssetId, encryptedAsset) in encryptedAssets {
            guard let descriptor = descriptorsByGlobalIdentifier[globalAssetId] else {
                log.critical("malformed descriptorsByGlobalIdentifier")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("malformed descriptorsByGlobalIdentifier")))
                return
            }
            
            for groupId in descriptor.sharingInfo.groupInfoById.keys {
                self.delegates.forEach({
                    $0.didStartDownloadOfAsset(withGlobalIdentifier: globalAssetId, in: groupId)
                })
            }
            
            do {
                let decryptedAsset = try localAssetsStore.decryptedAsset(
                    encryptedAsset: encryptedAsset,
                    quality: .lowResolution,
                    descriptor: descriptor
                )
                
                self.delegates.forEach({
                    $0.didFetchLowResolutionAsset(decryptedAsset)
                })
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegates.forEach({
                        $0.didCompleteDownloadOfAsset(
                            withGlobalIdentifier: encryptedAsset.globalIdentifier,
                            in: groupId
                        )
                    })
                }
            } catch {
                self.log.error("unable to decrypt local asset \(globalAssetId): \(error.localizedDescription)")
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegates.forEach({
                        $0.didFailDownloadOfAsset(
                            withGlobalIdentifier: encryptedAsset.globalIdentifier,
                            in: groupId,
                            with: error
                        )
                    })
                }
            }
        }
        
        completionHandler(.success(()))
    }
    
    /// For all the descriptors whose originator user is `self.user`, notify the restoration delegate
    /// about all groups that need to be restored along with the queue item identifiers for each `groupId`.
    /// Uploads and shares will be reported separately, according to the contract in the delegate.
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors keyed by asset global identifier
    ///   - completionHandler: the callback method
    func restoreQueueItems(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        ///
        /// Fetch from server users information (`SHServerUser` objects) for all user identifiers found in all descriptors
        ///
        var usersById = [String: SHServerUser]()
        var userIdentifiers = Set(descriptorsByGlobalIdentifier.values.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
        userIdentifiers.formUnion(Set(descriptorsByGlobalIdentifier.values.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
        
        do {
            usersById = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers)).reduce([:], { partialResult, user in
                var result = partialResult
                result[user.identifier] = user
                return result
            })
        } catch {
            self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var uploadQueueItemsIdsByGroupId = [String: [String]]()
        var shareQueueItemsIdsByGroupId = [String: [String]]()
        var userIdsInvolvedInRestoration = Set<String>()
        
        for (_, descriptor) in descriptorsByGlobalIdentifier {
            if descriptor.sharingInfo.sharedByUserIdentifier == user.identifier {
                // TODO: Is it possible for the local identifier not to exist? What happens when the asset is only on the server, and not in the local library?
                guard let localIdentifier = descriptor.localIdentifier else {
                    continue
                }
                
                if descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.count == 1,
                   let recipientUserId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.first,
                   recipientUserId == user.identifier {
                    ///
                    /// This handles assets just shared with self, meaning that they were just uploaded to the lockbox.
                    /// Restore the upload from the queue
                    ///
                    let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[recipientUserId]!
                    
                    let queueItemIdentifier = SHUploadPipeline.queueItemIdentifier(
                        groupId: groupId,
                        assetLocalIdentifier: localIdentifier,
                        versions: [.lowResolution, .hiResolution],
                        users: []
                    )
                    
                    if uploadQueueItemsIdsByGroupId[groupId] == nil {
                        uploadQueueItemsIdsByGroupId[groupId] = [queueItemIdentifier]
                    } else {
                        uploadQueueItemsIdsByGroupId[groupId]!.append(queueItemIdentifier)
                    }
                } else {
                    var userIdsByGroup = [String: [String]]()
                    for (userId, groupId) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                        guard userId != user.identifier else {
                            continue
                        }
                        if userIdsByGroup[groupId] == nil {
                            userIdsByGroup[groupId] = [userId]
                        } else {
                            userIdsByGroup[groupId]!.append(userId)
                        }
                    }
                    
                    for (groupId, userIds) in userIdsByGroup {
                        var queueItemIdentifiers = [String]()
                        
                        /// 
                        /// There are 2 possible cases:
                        /// 1. The asset was first uploaded, then shared with other users
                        ///     -> the .low and .hi resolutions were uploaded right away, no .mid. In this case the identifier only reference this user
                        /// 2. The asset was shared with other users before uploading
                        ///     -> the .low and .mid resolutions were uploaded first, then the .hi resolution
                        ///
                        /// Because of this, ask to restore all 3 combinations
                        ///
                        queueItemIdentifiers.append(
                            SHUploadPipeline.queueItemIdentifier(
                                groupId: groupId,
                                assetLocalIdentifier: localIdentifier,
                                versions: [.lowResolution, .hiResolution],
                                users: []
                            )
                        )
                        queueItemIdentifiers.append(
                            SHUploadPipeline.queueItemIdentifier(
                                groupId: groupId,
                                assetLocalIdentifier: localIdentifier,
                                versions: [.lowResolution, .midResolution],
                                users: userIds.map({ usersById[$0]! })
                            )
                        )
                        queueItemIdentifiers.append(
                            SHUploadPipeline.queueItemIdentifier(
                                groupId: groupId,
                                assetLocalIdentifier: localIdentifier,
                                versions: [.hiResolution],
                                users: userIds.map({ usersById[$0]! })
                            )
                        )
                        if uploadQueueItemsIdsByGroupId[groupId] == nil {
                            uploadQueueItemsIdsByGroupId[groupId] = queueItemIdentifiers
                        } else {
                            uploadQueueItemsIdsByGroupId[groupId]!.append(contentsOf: queueItemIdentifiers)
                        }
                        
                        ///
                        /// When sharing with other users, `.midResolution` is uploaded first with `.lowResolution`
                        /// and `.hiResolution` comes later.
                        ///
                        queueItemIdentifiers = [String]()
                        
                        queueItemIdentifiers.append(
                            SHUploadPipeline.queueItemIdentifier(
                                groupId: groupId,
                                assetLocalIdentifier: localIdentifier,
                                versions: [.lowResolution, .midResolution],
                                users: userIds.map({ usersById[$0]! })
                            )
                        )
                        queueItemIdentifiers.append(
                            SHUploadPipeline.queueItemIdentifier(
                                groupId: groupId,
                                assetLocalIdentifier: localIdentifier,
                                versions: [.hiResolution],
                                users: userIds.map({ usersById[$0]! })
                            )
                        )
                        
                        userIds.forEach({ userIdsInvolvedInRestoration.insert($0) })
                        
                        if shareQueueItemsIdsByGroupId[groupId] == nil {
                            shareQueueItemsIdsByGroupId[groupId] = queueItemIdentifiers
                        } else {
                            shareQueueItemsIdsByGroupId[groupId]!.append(contentsOf: queueItemIdentifiers)
                        }
                    }
                }
            }
        }
        
        self.log.debug("upload queue items by group \(uploadQueueItemsIdsByGroupId)")
        self.log.debug("share queue items by group \(shareQueueItemsIdsByGroupId)")
        
        for (groupId, queueItemIdentifiers) in uploadQueueItemsIdsByGroupId {
            self.restorationDelegate.restoreUploadQueueItems(withIdentifiers: queueItemIdentifiers, in: groupId)
        }
        
        for (groupId, queueItemIdentifiers) in shareQueueItemsIdsByGroupId {
            self.restorationDelegate.restoreShareQueueItems(withIdentifiers: queueItemIdentifiers, in: groupId)
        }
        
        self.restorationDelegate.didCompleteRestoration(
            userIdsInvolvedInRestoration: Array(userIdsInvolvedInRestoration)
        )
        
        completionHandler(.success(()))
    }
    
    internal override func processAssetsInDescriptors(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
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
        
        self.restoreQueueItems(descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier) { _ in }
        
        self.mergeDescriptorsWithApplePhotosAssets(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier
        ) { result in
            switch result {
            case .success(let nonLocalPhotoLibraryDescriptorsByGlobalIdentifier):
                let start = CFAbsoluteTimeGetCurrent()
                
                ///
                /// Get the encrypted assets for the ones not found in the apple photos library to start decryption.
                ///
                self.decryptFromLocalStore(
                    descriptorsByGlobalIdentifier: nonLocalPhotoLibraryDescriptorsByGlobalIdentifier
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
    
    ///
    /// Fetch descriptors from local server.
    /// Filters blacklisted assets and users.
    /// Filter out non-completed uploads.
    /// Return the descriptors for the assets to download keyed by global identifier.
    ///
    /// - Parameter completionHandler: the callback
    internal override func processDescriptors(
        completionHandler: @escaping (Swift.Result<[String: SHAssetDescriptor], Error>) -> Void
    ) {
        var descriptors = [any SHAssetDescriptor]()
        do {
            descriptors = try self.fetchDescriptorsFromServer()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        ///
        /// Filter out the ones that were blacklisted
        ///
        descriptors = descriptors.filter {
            DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: $0.globalIdentifier) == false
            && DownloadBlacklist.shared.isBlacklisted(userIdentifier: $0.sharingInfo.sharedByUserIdentifier) == false
        }
        
        guard descriptors.count > 0 else {
            completionHandler(.success([:]))
            return
        }
        
        var descriptorsByGlobalIdentifier = [String: any SHAssetDescriptor]()
        for descriptor in descriptors {
            ///
            /// Filter-out non-completed uploads
            ///
            guard descriptor.uploadState == .completed else {
                continue
            }
            descriptorsByGlobalIdentifier[descriptor.globalIdentifier] = descriptor
        }
        completionHandler(.success(descriptorsByGlobalIdentifier))
    }
    
    ///
    /// Get all asset descriptors associated with this user from the server.
    /// Descriptors serve as a manifest to determine what to download.
    /// For assets that exist in the local Apple Photos Library, do not decrypt, but just serve the corresponding PHAsset
    ///
    /// ** Note that only .lowResolution assets are fetched from the local server here.**
    /// ** Higher resolutions are meant to be lazy loaded by the delegate.**
    ///
    /// - Parameter completionHandler: the callback method
    public func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        self.processDescriptors { result in
            switch result {
            case .failure(let error):
                self.log.error("failed to fetch local descriptors: \(error.localizedDescription)")
                self.delegates.forEach({
                    $0.didCompleteDownloadCycle(with: .failure(error))
                })
                completionHandler(.failure(error))
            case .success(let descriptorsByGlobalIdentifier):
                self.processAssetsInDescriptors(descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier) {
                    secondResult in
                    self.delegates.forEach({
                        $0.didCompleteDownloadCycle(with: secondResult)
                    })
                    completionHandler(secondResult)
                }
            }
        }
    }
}
