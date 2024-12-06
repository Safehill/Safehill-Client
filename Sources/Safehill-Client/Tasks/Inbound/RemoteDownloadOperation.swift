import Foundation
import KnowledgeBase
import os

///
/// This pipeline operation is responsible for keeping assets in the remote server and local server in sync.
/// It **ignores all assets in the local server**, except for when determining the set it needs to operate on.
/// The restoration delegates are notified about successful uploads and shares from this user. If these items are not present in the history queues, they are created and persisted there.
///
///
/// The steps are:
/// 1. `fetchDescriptors(filteringAssets:filteringGroups:after:completionHandler:)` : the descriptors are fetched from both the remote and local servers to determine the ones to operate on, namely the ones ONLY on remote. The incompletes (with upload state `.notStarted`) are carried over in each iteration, otherwise they would be filtered out by the  `afterDate` filter and missed forever
/// 2. `processDescriptors(_:fromRemote:qos:completionHandler:)` : all user referenced in the descriptor that can't be retrived from server are filtered out. If sender can't be retrieved the whole descriptor is filtered out
/// 3. `processAssetsInDescriptors(descriptorsByGlobalIdentifier:qos:completionHandler:)` :
///     - local server assets and queue items are created when missing, and the restoration delegate is called
///
public class SHRemoteDownloadOperation: Operation, SHBackgroundOperationProtocol, SHDownloadOperation, @unchecked Sendable {
    
    internal static var lastFetchDate: Date? = nil
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.download.delegates")
    
    let user: SHLocalUserProtocol
    
    let downloaderDelegates: [SHAssetDownloaderDelegate]
    let restorationDelegate: SHAssetActivityRestorationDelegate
    
    let photoIndexer: SHPhotosIndexer
    
    public init(user: SHLocalUserProtocol,
                downloaderDelegates: [SHAssetDownloaderDelegate],
                restorationDelegate: SHAssetActivityRestorationDelegate,
                photoIndexer: SHPhotosIndexer) {
        self.user = user
        self.downloaderDelegates = downloaderDelegates
        self.restorationDelegate = restorationDelegate
        self.photoIndexer = photoIndexer
    }
    
    var serverProxy: SHServerProxy { self.user.serverProxy }
    
    internal func fetchDescriptors(
        filteringAssets globalIdentifiers: [GlobalIdentifier]? = nil,
        filteringGroups groupIds: [String]? = nil,
        after date: Date?,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        let afterDate = date ?? SHRemoteDownloadOperation.lastFetchDate
        
        self.log.debug("[\(type(of: self))] fetchDescriptors for \(globalIdentifiers ?? []) filteringGroups=\(groupIds ?? []) after \(afterDate?.iso8601withFractionalSeconds ?? "nil")")
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        self.serverProxy.getRemoteAssetDescriptors(
            for: (globalIdentifiers?.isEmpty ?? true) ? nil : globalIdentifiers!,
            after: afterDate,
            filteringGroups: groupIds
        ) { remoteResult in
            switch remoteResult {
            case .success(let remoteDescriptors):
                
                guard remoteDescriptors.isEmpty == false else {
                    completionHandler(.success([]))
                    return
                }
                
                ///
                /// Get all the corresponding local descriptors.
                /// The extra ones (to be DELETED) will be removed by the sync operation
                /// The ones that changed (to be UPDATED) will be changed by the sync operation
                ///
                self.serverProxy.getLocalAssetDescriptors(
                    for: remoteDescriptors.map { $0.globalIdentifier },
                    useCache: false
                ) { localResult in
                    switch localResult {
                    case .success(let localDescriptors):
                        let onlyRemoteDescriptors = remoteDescriptors.filter({ remoteDesc in
                            localDescriptors.contains(where: { $0.globalIdentifier == remoteDesc.globalIdentifier }) == false
                        })
                        
                        completionHandler(.success(onlyRemoteDescriptors))
                        
                    case .failure(let err):
                        self.log.error("[\(type(of: self))] failed to fetch descriptors from LOCAL server when syncing: \(err.localizedDescription)")
                        completionHandler(.failure(err))
                    }
                }
                
            case .failure(let err):
                self.log.error("[\(type(of: self))] failed to fetch descriptors from REMOTE server: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
    
    internal func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier],
        completionHandler: @escaping (Result<[UserIdentifier: any SHServerUser], Error>) -> Void
    ) {
        SHUsersController(localUser: self.user).getUsers(withIdentifiers: userIdentifiers, completionHandler: completionHandler)
    }
    
    ///
    /// Takes the full list of descriptors from server (remote or local, depending on whether it's running as part of the
    /// `SHLocalActivityRestoreOperation` or the `SHDownloadOperation`.
    ///
    ///
    /// Filters out
    /// - the ones referencing blacklisted assets (items that have been tried to download too many times),
    /// - the ones where any of the users referenced can't be retrieved
    /// - the ones for which the upload hasn't started
    ///
    /// Call the delegate with the full manifest of assets shared by OTHER users.
    /// Returns the full set of descriptors fetched from the server, keyed by global identifier.
    ///
    /// - Parameters:
    ///   - descriptors: the descriptors to process
    ///   - priority: the task priority.  Usually`.high` for initial restoration at start, `.background` for background downloads
    ///   - completionHandler: the callback
    ///
    internal func processDescriptors(
        _ descriptors: [any SHAssetDescriptor],
        fromRemote: Bool,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHAssetDescriptor], Error>) -> Void
    ) {
        ///
        /// Filter out the ones:
        /// - whose assets were blacklisted
        /// - whose users were blacklisted
        /// - haven't started upload
        ///
        Task(priority: qos.toTaskPriority()) {
            guard !self.isCancelled else {
                log.info("[\(type(of: self))] download task cancelled. Finishing")
                completionHandler(.success([:]))
                return
            }
            
            let filteredDescriptors = descriptors.filter {
                $0.uploadState != .failed && $0.uploadState != .notStarted
            }
            
#if DEBUG
            if filteredDescriptors.count != descriptors.count {
                let incomplete = descriptors.filter {
                    $0.uploadState == .notStarted || $0.uploadState == .failed
                }
                if incomplete.isEmpty == false {
                    log.debug("[\(type(of: self))] filtering out incomplete gids \(incomplete.map({ $0.globalIdentifier }))")
                }
            }
#endif
            
            guard filteredDescriptors.count > 0 else {
                let downloaderDelegates = self.downloaderDelegates
                self.delegatesQueue.async {
                    if fromRemote {
                        downloaderDelegates.forEach({
                            $0.didReceiveRemoteAssetDescriptors([], referencing: [:])
                        })
                    } else {
                        downloaderDelegates.forEach({
                            $0.didReceiveLocalAssetDescriptors([], referencing: [:])
                        })
                    }
                }
                completionHandler(.success([:]))
                return
            }
            
            ///
            /// Fetch from server users information (`SHServerUser` objects)
            /// for all user identifiers found in all descriptors shared by OTHER known users
            ///
            var userIdentifiers = Set(filteredDescriptors.flatMap { $0.sharingInfo.groupIdsByRecipientUserIdentifier.keys })
            userIdentifiers.formUnion(filteredDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier })
            
            self.getUsers(withIdentifiers: Array(userIdentifiers)) { result in
                switch result {
                case .failure(let error):
                    self.log.error("[\(type(of: self))] unable to fetch users from local server: \(error.localizedDescription)")
                    completionHandler(.failure(error))
                case .success(let usersDict):
                    let filteredDescriptorsFromRetrievableUsers: [any SHAssetDescriptor]
                    
                    if fromRemote {
                        ///
                        /// Filter out the descriptor that reference any user that could not be retrieved
                        ///
                        filteredDescriptorsFromRetrievableUsers = filteredDescriptors.compactMap {
                            (desc: any SHAssetDescriptor) -> (any SHAssetDescriptor)? in
                            if usersDict[desc.sharingInfo.sharedByUserIdentifier] == nil {
                                return nil
                            }
                            
                            var newSharedWith = desc.sharingInfo.groupIdsByRecipientUserIdentifier
                            
                            for sharedWithUserId in desc.sharingInfo.groupIdsByRecipientUserIdentifier.keys {
                                if usersDict[sharedWithUserId] == nil {
                                    newSharedWith.removeValue(forKey: sharedWithUserId)
                                }
                            }
                            
                            return SHGenericAssetDescriptor(
                                globalIdentifier: desc.globalIdentifier,
                                localIdentifier: desc.localIdentifier,
                                creationDate: desc.creationDate,
                                uploadState: desc.uploadState,
                                sharingInfo: SHGenericDescriptorSharingInfo(
                                    sharedByUserIdentifier: desc.sharingInfo.sharedByUserIdentifier,
                                    groupIdsByRecipientUserIdentifier: newSharedWith,
                                    groupInfoById: desc.sharingInfo.groupInfoById
                                )
                            )
                        }
                    } else {
                        filteredDescriptorsFromRetrievableUsers = filteredDescriptors
                    }
                    
#if DEBUG
                    if filteredDescriptorsFromRetrievableUsers.count != filteredDescriptors.count {
                        let filtered = filteredDescriptorsFromRetrievableUsers.map({ $0.globalIdentifier })
                        let unfiltered = filteredDescriptors
                            .map({ $0.globalIdentifier })
                            .filter {
                                filtered.contains($0) == false
                            }
                        self.log.debug("[\(type(of: self))] filtering out gids with unretrievable users \(unfiltered)")
                    }
#endif
                    
                    ///
                    /// Call the delegate with the full manifest of whitelisted assets.
                    /// The ones shared by THIS user will be restored through the restoration delegate.
                    ///
                    let userDictsImmutable = usersDict
                    let downloaderDelegates = self.downloaderDelegates
                    self.delegatesQueue.async {
                        if fromRemote {
                            downloaderDelegates.forEach({
                                $0.didReceiveRemoteAssetDescriptors(
                                    filteredDescriptorsFromRetrievableUsers,
                                    referencing: userDictsImmutable
                                )
                            })
                        } else {
                            downloaderDelegates.forEach({
                                $0.didReceiveLocalAssetDescriptors(
                                    filteredDescriptorsFromRetrievableUsers,
                                    referencing: userDictsImmutable
                                )
                            })
                        }
                    }
                    
                    let descriptorsByGlobalIdentifier = filteredDescriptorsFromRetrievableUsers.reduce(
                        [String: any SHAssetDescriptor]()
                    ) { partialResult, descriptor in
                        var result = partialResult
                        result[descriptor.globalIdentifier] = descriptor
                        return result
                    }
                    completionHandler(.success(descriptorsByGlobalIdentifier))
                }
            }
        }
    }
    
    /// Given a list of descriptors determines which ones need to be dowloaded, authorized, or marked as backed up in the library.
    /// Returns the list of descriptors for the assets that are ready to be downloaded
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: the descriptors, keyed by asset global identifier
    ///   - completionHandler: the callback
    internal func processAssetsInDescriptors(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHAssetDescriptor], Error>) -> Void
    ) {
        guard !self.isCancelled else {
            log.info("[\(type(of: self))] download task cancelled. Finishing")
            completionHandler(.success([:]))
            return
        }
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success([:]))
            return
        }
        
        let sharedBySelfDescriptorsByGlobalIdentifier = descriptorsByGlobalIdentifier.filter({
            dict in
            dict.value.sharingInfo.sharedByUserIdentifier == self.user.identifier
        })
        
        self.restoreQueueItems(
            descriptorsByGlobalIdentifier: sharedBySelfDescriptorsByGlobalIdentifier,
            qos: qos
        ) { restoreResult in
            switch restoreResult {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success:
                completionHandler(.success(descriptorsByGlobalIdentifier))
            }
        }
    }
    
    private func process(
        _ descriptorsForItemsToDownload: [any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHAssetDescriptor], Error>) -> Void
    ) {
        guard !self.isCancelled else {
            log.info("[\(type(of: self))] download task cancelled. Finishing")
            completionHandler(.success([:]))
            return
        }
        
        ///
        /// This processing takes care of the **CREATES**, namely the new assets on the server not present locally
        /// Given the descriptors that are only on REMOTE, determine what needs to be downloaded after filtering
        ///
        
        guard descriptorsForItemsToDownload.isEmpty == false else {
            completionHandler(.success([:]))
            return
        }
        
        self.log.debug("[\(type(of: self))] original descriptors: \(descriptorsForItemsToDownload.count)")
        
        self.processDescriptors(
            descriptorsForItemsToDownload,
            fromRemote: true,
            qos: qos
        ) { descResult in
            
            switch descResult {
                
            case .failure(let err):
                self.log.error("[\(type(of: self))] failed to download descriptors: \(err.localizedDescription)")
                completionHandler(.failure(err))
                
            case .success(let filteredDescriptorsByAssetGid):
                
#if DEBUG
                let delta = Set(descriptorsForItemsToDownload.map({ $0.globalIdentifier })).subtracting(filteredDescriptorsByAssetGid.keys)
                self.log.debug("[\(type(of: self))] after processing: \(filteredDescriptorsByAssetGid.count). delta=\(delta)")
#endif
                
                self.processAssetsInDescriptors(
                    descriptorsByGlobalIdentifier: filteredDescriptorsByAssetGid,
                    qos: qos
                ) { descAssetResult in
                    switch descAssetResult {
                        
                    case .failure(let err):
                        self.log.error("[\(type(of: self))] failed to process assets in descriptors: \(err.localizedDescription)")
                        completionHandler(.failure(err))
                        
                    case .success(let descriptorsReadyToDownloadById):
#if DEBUG
                        let delta1 = Set(filteredDescriptorsByAssetGid.keys).subtracting(descriptorsReadyToDownloadById.map({ $0.value.globalIdentifier }))
                        let delta2 = Set(descriptorsReadyToDownloadById.map({ $0.value.globalIdentifier })).subtracting(filteredDescriptorsByAssetGid.keys)
                        self.log.debug("[\(type(of: self))] ready for download: \(descriptorsReadyToDownloadById.count). onlyInProcessed=\(delta1) onlyInToDownload=\(delta2)")
#endif
                        
                        guard descriptorsReadyToDownloadById.isEmpty == false else {
                            completionHandler(.success([:]))
                            return
                        }
                        
                        do {
                            let descriptorsReadyToDownload = Array(descriptorsReadyToDownloadById.values)
                            try SHKGQuery.ingest(descriptorsReadyToDownload, receiverUserId: self.user.identifier)
                            completionHandler(.success(descriptorsReadyToDownloadById))
                        } catch {
                            completionHandler(.failure(error))
                            return
                        }
                    }
                }
            }
        }
    }
    
    public func run(
        for assetGlobalIdentifiers: [GlobalIdentifier]?,
        filteringGroups groupIds: [String]?,
        startingFrom date: Date?,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHAssetDescriptor], Error>) -> Void
    ) {
        let fetchStartedAt = Date()
        
        let handleResult = { (result: Result<[GlobalIdentifier: any SHAssetDescriptor], Error>) in
            let downloaderDelegates = self.downloaderDelegates
            
            switch result {
            
            case .failure(let error):
                self.delegatesQueue.async {
                    downloaderDelegates.forEach({
                        $0.didFailDownloadCycle(with: error)
                    })
                }
                
            case .success(let dict):
                SHRemoteDownloadOperation.lastFetchDate = fetchStartedAt
                self.delegatesQueue.async {
                    downloaderDelegates.forEach({
                        $0.didCompleteDownloadCycle(
                            forLocalDescriptors: dict
                        )
                    })
                }
            }
            
            completionHandler(result)
        }
        
        guard self.user is SHAuthenticatedLocalUser else {
            handleResult(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        DispatchQueue.global(qos: qos).async {
            self.fetchDescriptors(
                filteringAssets: assetGlobalIdentifiers,
                filteringGroups: groupIds,
                after: date
            ) {
                (result: Result<([any SHAssetDescriptor]), Error>) in
                switch result {
                case .success(let descriptorsForItemsToDownload):
                    self.process(
                        descriptorsForItemsToDownload,
                        qos: qos,
                        completionHandler: handleResult
                    )
                case .failure(let error):
                    handleResult(.failure(error))
                }
            }
        }
    }
    
    public func run(
        startingFrom date: Date?,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.run(
            for: nil,
            filteringGroups: nil,
            startingFrom: date,
            qos: qos
        ) { result in
            if case .failure(let failure) = result {
                completionHandler(.failure(failure))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    public func run(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.run(startingFrom: nil, qos: qos, completionHandler: completionHandler)
    }
}

public let RemoteDownloadPipelineProcessor = SHBackgroundOperationProcessor<SHRemoteDownloadOperation>()
