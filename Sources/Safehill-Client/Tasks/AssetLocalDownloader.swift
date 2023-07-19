import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


public class SHLocalDownloadOperation: SHDownloadOperation {
    
    let assetGlobalIdentifiersInCache: [GlobalIdentifier]
    
    override public init(user: SHLocalUser,
                         delegate: SHAssetDownloaderDelegate,
                         outboundDelegates: [SHOutboundAssetOperationDelegate],
                         limitPerRun limit: Int? = nil) {
        self.assetGlobalIdentifiersInCache = []
        super.init(user: user, delegate: delegate, outboundDelegates: outboundDelegates, limitPerRun: limit)
    }
    
    public init(user: SHLocalUser,
                assetGlobalIdentifiersInCache: [GlobalIdentifier],
                delegate: SHAssetDownloaderDelegate,
                outboundDelegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int? = nil) {
        self.assetGlobalIdentifiersInCache = assetGlobalIdentifiersInCache
        super.init(user: user, delegate: delegate, outboundDelegates: outboundDelegates, limitPerRun: limit)
    }
    
    private func fetchLocalDescriptors(completionHandler: @escaping (Swift.Result<[String: SHAssetDescriptor], Error>) -> Void) {
        ///
        /// Fetching assets from the ServerProxy is a 2-step process
        /// 1. Get the descriptors (no data) to determine which assets to pull. This calls the delegate with (Assets.downloading)
        /// 2. Get the assets data for the assets not already downloaded (based on the descriptors), and call the delegate with Assets with the low-rez `encryptedData` first (synchronously), then with the hi-rez (asynchronously, in a background thread)
        ///
        
        let fetchResult: (
            appleLibraryIdentifiers: [String],
            localDescriptors: [any SHAssetDescriptor],
            remoteDescriptors: [any SHAssetDescriptor]
        )
        do { fetchResult = try self.fetchDescriptors(skipRemote: true) }
        catch {
            completionHandler(.failure(error))
            return
        }
        
        let existingGlobalIdentifiers = self.assetGlobalIdentifiersInCache
        let existingLocalIdentifiers = fetchResult.appleLibraryIdentifiers
        
        let start = CFAbsoluteTimeGetCurrent()
        
        var descriptorsByLocalIdentifier = [String: any SHAssetDescriptor]()
        var descriptorsByGlobalIdentifier = [String: any SHAssetDescriptor]()
        
        var globalIdentifiersToFetchFromLocalServer = [String]()
        
        for descriptor in fetchResult.localDescriptors {
            if let localIdentifier = descriptor.localIdentifier,
               existingLocalIdentifiers.contains(localIdentifier) {
                descriptorsByLocalIdentifier[localIdentifier] = descriptor
            } else {
                guard existingGlobalIdentifiers.contains(descriptor.globalIdentifier) == false else {
                    continue
                }
                
                globalIdentifiersToFetchFromLocalServer.append(descriptor.globalIdentifier)
            }
        }
        
        if descriptorsByLocalIdentifier.count > 0 {
            self.delegate.markLocalAssetsAsUploaded(descriptorsByLocalIdentifier: descriptorsByLocalIdentifier)
        }
        
        if globalIdentifiersToFetchFromLocalServer.count == 0 {
            completionHandler(.success([:]))
            return
        }
        
        descriptorsByGlobalIdentifier = fetchResult.localDescriptors.filter {
            globalIdentifiersToFetchFromLocalServer.contains($0.globalIdentifier)
        }.reduce([:]) { partialResult, descriptor in
            var result = partialResult
            result[descriptor.globalIdentifier] = descriptor
            return result
        }
        
        ///
        /// Fetch from server users information (`SHServerUser` objects) for all user identifiers found in all descriptors
        ///
        
        var users = [SHServerUser]()
        var userIdentifiers = Set(fetchResult.localDescriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
        userIdentifiers.formUnion(Set(fetchResult.localDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
        
        do {
            users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
        } catch {
            self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        let descriptors = Array(descriptorsByGlobalIdentifier.values)
        if descriptors.count > 0 {
            self.delegate.handleAssetDescriptorResults(for: descriptors, users: users)
        }
        
        let end = CFAbsoluteTimeGetCurrent()
        self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(descriptors.count) descriptors")
        
        completionHandler(.success(descriptorsByGlobalIdentifier))
    }
    
    private func findGroupIdForSelfUser(for descriptor: any SHAssetDescriptor) -> String? {
        var groupId: String? = nil
        for (userId, gid) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
            if userId == user.identifier {
                groupId = gid
                break
            }
        }
        guard let groupId = groupId else {
            log.warning("The asset descriptor sharing information doesn't seem to include the event originator")
            return nil
        }
        return groupId
    }
    
    public func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        /// ** Note that only .lowResolution assets are fetched from the local server here.**
        /// ** Higher resolutions are meant to be lazy loaded by the delegate.**
        ///
        self.fetchLocalDescriptors { result in
            switch result {
            case .failure(let error):
                self.log.error("failed to fetch local descriptors: \(error.localizedDescription)")
                completionHandler(.failure(error))
            case .success(let descriptorsByGlobalIdentifier):
                ///
                /// Get all asset descriptors associated with this user from the server.
                /// Descriptors serve as a manifest to determine what to download
                ///
                guard descriptorsByGlobalIdentifier.count > 0 else {
                    completionHandler(.success(()))
                    return
                }
                
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
                
                for (assetId, encryptedAsset) in encryptedAssets {
                    guard let descriptor = descriptorsByGlobalIdentifier[assetId] else {
                        fatalError("malformed descriptorsByGlobalIdentifier")
                    }
                    guard let groupId = self.findGroupIdForSelfUser(for: descriptor) else {
                        continue
                    }
                    
                    do {
                        let decryptedAsset = try localAssetsStore.decryptedAsset(
                            encryptedAsset: encryptedAsset,
                            quality: .lowResolution,
                            descriptor: descriptor
                        )
                        
                        self.delegate.handleLowResAsset(decryptedAsset)
                        self.delegate.completed(decryptedAsset.globalIdentifier, groupId: groupId)
                    } catch {
                        self.log.error("unable to decrypt local asset \(assetId): \(error.localizedDescription)")
                    }
                }
                
                completionHandler(.success(()))
            }
        }
    }
}
