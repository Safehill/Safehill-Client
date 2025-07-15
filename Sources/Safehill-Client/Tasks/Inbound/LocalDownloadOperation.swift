import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


///
/// This pipeline operation deals with assets present the LocalServer.
/// **It does not** deal with remote server or remote descriptors. See `SHRemoteDownloadOperation` for remote descriptor processing.
/// The local pipeline is responsible for identifying the mapping between the assets in the photos library and the assets in the local server. The ones that don't have a mapping to the local photo library,  are decrypted from the local server and served via the `SHAssetSyncingDelegate`.
///
///
/// The steps are:
/// 1. `fetchDescriptors(filteringAssets:filteringGroups:after:completionHandler:)`: the descriptors are fetched from the local server
/// 2. Convert descriptors into `AssetActivity` objects and calling the `SHActivitySyncingDelegate`
/// 3. `processDescriptors(_:fromRemote:qos:completionHandler:)` : descriptors are filtered our if
///     - the referenced asset is blacklisted (attemtped to download too many times),
///     - any user referenced in the descriptor is not "retrievabile", or
///     - the asset hasn't finished uploaing (upload status is neither `.notStarted` nor `.failed`)
/// 4. `processAssetsInDescriptors(descriptorsByGlobalIdentifier:qos:completionHandler:)` :
///     - the assets shared by from _other_ users are returned so they can be decrypted
///
/// Ideally in the lifecycle of the application, the decryption of the low resolution happens only once.
/// The delegate is responsible for keeping these decrypted assets in memory, or call the `SHServerProxy` to retrieve them again if they are disposed.
///
/// Any new asset that needs to be downloaded from the server while the app is running is taken care of by the sister processor, the `SHRemoteDownloadOperation`.
///
internal class SHLocalDownloadOperation: SHRemoteDownloadOperation, @unchecked Sendable {
    
    public override init(
        user: SHLocalUserProtocol,
        assetSyncingDelegates: [SHAssetSyncingDelegate],
        activitySyncingDelegates: [SHActivitySyncingDelegate],
        photoIndexer: SHPhotosIndexer
    ) {
        super.init(
            user: user,
            assetSyncingDelegates: assetSyncingDelegates,
            activitySyncingDelegates: activitySyncingDelegates,
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
        self.fetchDescriptors(after: date) {
            result in
            switch result {
            case .failure(let error):
                self.log.error("[\(type(of: self))] failed to fetch local descriptors: \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            case .success(let fullDescriptorList):
                self.log.debug("[\(type(of: self))] original descriptors: \(fullDescriptorList.count)")
                self.processDescriptors(fullDescriptorList, fromRemote: false, qos: qos) { result in
                    switch result {
                    case .failure(let error):
                        self.log.error("[\(type(of: self))] failed to process descriptors: \(error.localizedDescription)")
                        completionHandler(.failure(error))
                    case .success(let filteredDescriptors):
#if DEBUG
                        let delta = Set(fullDescriptorList.map({ $0.globalIdentifier })).subtracting(filteredDescriptors.keys)
                        self.log.debug("[\(type(of: self))] after processing: \(filteredDescriptors.count). delta=\(delta)")
#endif
                        completionHandler(.success(()))
                    }
                }
            }
        }
    }
}
