import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


///
/// This pipeline operation deals with assets present the LocalServer.
/// **It does not** deal with remote server or remote descriptors. See `SHRemoteDownloadOperation` for remote descriptor processing.
/// The local pipeline is responsible for identifying the mapping between the assets in the photos library and the assets in the local server. The ones that don't have a mapping to the local photo library,  are decrypted from the local server and served via the `SHAssetDownloaderDelegate`.
/// The restoration delegates are notified about successful uploads and shares from this user. It assumes the history queues on disk are in sync with the local server, so it doesn't try to create history items like the `SHRemoteDownloadOperation`.
///
///
/// The steps are:
/// 1. `fetchDescriptors(filteringAssets:filteringGroups:after:completionHandler:)`: the descriptors are fetched from the local server
/// 2. `processDescriptors(_:fromRemote:qos:completionHandler:)` : descriptors are filtered our if
///     - the referenced asset is blacklisted (attemtped to download too many times),
///     - any user referenced in the descriptor is not "retrievabile", or
///     - the asset hasn't finished uploaing (upload status is neither `.notStarted` nor `.failed`)
/// 3. `processAssetsInDescriptors(descriptorsByGlobalIdentifier:qos:completionHandler:)` :
///     - for the assets shared by _this_ user, the restoration delegate is called to restore them
///     - the assets shared by from _other_ users are returned so they can be decrypted
///
/// Ideally in the lifecycle of the application, the decryption of the low resolution happens only once.
/// The delegate is responsible for keeping these decrypted assets in memory, or call the `SHServerProxy` to retrieve them again if they are disposed.
///
/// Any new asset that needs to be downloaded from the server while the app is running is taken care of by the sister processor, the `SHRemoteDownloadOperation`.
///
public class SHLocalDownloadOperation: SHRemoteDownloadOperation, @unchecked Sendable {
    
    public override init(
        user: SHLocalUserProtocol,
        downloaderDelegates: [SHAssetDownloaderDelegate],
        restorationDelegate: SHAssetActivityRestorationDelegate,
        photoIndexer: SHPhotosIndexer
    ) {
        super.init(
            user: user,
            downloaderDelegates: downloaderDelegates,
            restorationDelegate: restorationDelegate,
            photoIndexer: photoIndexer
        )
    }
    
    /// This method overrides the behavior of the `SHRemoteDownloadOperation` 
    /// to make the descriptor fetch happen against the local server (instead of the remote server)
    ///
    /// - Returns: the list of descriptors
    ///
    override internal func fetchDescriptors(
        filteringAssets globalIdentifiers: [GlobalIdentifier]? = nil,
        filteringGroups groupIds: [String]? = nil,
        after date: Date?,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        self.log.debug("[\(type(of: self))] fetchDescriptors for \(globalIdentifiers ?? []) filteringGroups=\(groupIds ?? []) after \(date?.iso8601withFractionalSeconds ?? "nil")")
        
        serverProxy.getLocalAssetDescriptors(after: date) { result in
            switch result {
            case .success(let descs):
                self.log.debug("[\(type(of: self))] fetched descriptors for gids \(descs.map({ $0.globalIdentifier }))")
                completionHandler(.success(descs))
                
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    internal override func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier],
        completionHandler: @escaping (Result<[UserIdentifier: any SHServerUser], Error>) -> Void
    ) {
        Task {
            do {
                let usersDict = try await SHUsersController(localUser: self.user).getUsersOrCached(with: userIdentifiers)
                completionHandler(.success(usersDict))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
    
    override internal func restore(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard !self.isCancelled else {
            log.info("[\(type(of: self))] download task cancelled. Finishing")
            completionHandler(.success(()))
            return
        }
        
        ///
        /// FOR THE ONES SHARED BY THIS USER
        /// Notify the restoration delegates about the assets that need to be restored from the successful queues
        /// These can only reference assets shared by THIS user. All other descriptors are ignored.
        ///
        /// These assets are definitely in the local server (because the descriptors fetch by a `SHLocalDownloadOperation`
        /// contain only assets from the local server).
        /// However they may or may not be in the history queues. The method `restoreQueueItems(descriptorsByGlobalIdentifier:filteringKeys:)
        /// takes care of creating them
        ///
        self.restoreQueueItems(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
            filteringKeys: filteringKeys
        ) { restoreResult in
            
            switch restoreResult {
            case .success:
                completionHandler(.success(()))

            case .failure(let error):
                self.log.critical("failure while restoring queue items \(filteringKeys): \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
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
    public override func run(
        startingFrom date: Date?,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let handleFailure = { (error: Error) in
            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                downloaderDelegates.forEach({
                    $0.didFailDownloadCycle(with: error)
                })
            }
            completionHandler(.failure(error))
        }
        
        self.fetchDescriptors(after: date) {
            result in
            switch result {
            case .failure(let error):
                self.log.error("[\(type(of: self))] failed to fetch local descriptors: \(error.localizedDescription)")
                handleFailure(error)
                return
            case .success(let fullDescriptorList):
                self.log.debug("[\(type(of: self))] original descriptors: \(fullDescriptorList.count)")
                self.processDescriptors(fullDescriptorList, fromRemote: false, qos: qos) { result in
                    switch result {
                    case .failure(let error):
                        self.log.error("[\(type(of: self))] failed to process descriptors: \(error.localizedDescription)")
                        handleFailure(error)
                    case .success(let filteredDescriptors):
#if DEBUG
                        let delta = Set(fullDescriptorList.map({ $0.globalIdentifier })).subtracting(filteredDescriptors.keys)
                        self.log.debug("[\(type(of: self))] after processing: \(filteredDescriptors.count). delta=\(delta)")
#endif
                        self.processAssetsInDescriptors(
                            descriptorsByGlobalIdentifier: filteredDescriptors,
                            qos: qos
                        ) { result in
                            
                            switch result {
                            case .success(let descriptorsReadyToDecryptById):
                                
#if DEBUG
                                let delta1 = Set(filteredDescriptors.keys).subtracting(descriptorsReadyToDecryptById.map({ $0.value.globalIdentifier }))
                                let delta2 = Set(descriptorsReadyToDecryptById.map({ $0.value.globalIdentifier })).subtracting(filteredDescriptors.keys)
                                self.log.debug("[\(type(of: self))] ready for decryption: \(descriptorsReadyToDecryptById.count). onlyInProcessed=\(delta1) onlyInToDecrypt=\(delta2)")
#endif
                                
                                let downloaderDelegates = self.downloaderDelegates
                                downloaderDelegates.forEach({
                                    $0.didCompleteDownloadCycle(
                                        forLocalDescriptors: descriptorsReadyToDecryptById
                                    )
                                })
                                
                                completionHandler(.success(()))
                                
                            case .failure(let error):
                                handleFailure(error)
                            }
                            
                        }
                    }
                }
            }
        }
    }
}

public let LocalDownloadPipelineProcessor = SHBackgroundOperationProcessor<SHLocalDownloadOperation>()
