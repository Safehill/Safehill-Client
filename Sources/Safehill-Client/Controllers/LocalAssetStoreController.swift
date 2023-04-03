import Foundation

public typealias LocalIdentifier = String


public struct SHLocalAssetStoreController {
    public let user: SHLocalUser
    
    public init(user: SHLocalUser) {
        self.user = user
    }
    
    private var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func globalIdentifiers() -> [GlobalIdentifier] {
        var identifiersInCache = [String]()
        
        let group = DispatchGroup()
        group.enter()
        
        self.serverProxy.getLocalAssetDescriptors { result in
            switch result {
            case .success(let descriptors):
                identifiersInCache = descriptors.map { $0.globalIdentifier }
            case .failure(let err):
                log.error("failed to get local asset descriptors: \(err.localizedDescription)")
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        if dispatchResult == .timedOut {
            log.error("the background download operation timed out")
        }
        return identifiersInCache
    }
    
    public func encryptedAsset(
        with globalIdentifier: GlobalIdentifier,
        versions: [SHAssetQuality]? = nil,
        cacheHiResolution: Bool
    ) throws -> any SHEncryptedAsset
    {
        let result = try self.encryptedAssets(with: [globalIdentifier],
                                              versions: versions,
                                              cacheHiResolution: cacheHiResolution)
        guard let asset = result[globalIdentifier] else {
            throw SHBackgroundOperationError.missingAssetInLocalServer(globalIdentifier)
        }
        return asset
    }
    
    public func encryptedAssets(
        with globalIdentifiers: [GlobalIdentifier],
        versions: [SHAssetQuality]? = nil,
        cacheHiResolution: Bool
    ) throws -> [GlobalIdentifier: any SHEncryptedAsset]
    {
        var assets = [GlobalIdentifier: any SHEncryptedAsset]()
        var error: Error? = nil
        
        let group = DispatchGroup()
        group.enter()
        self.serverProxy.getLocalAssets(
            withGlobalIdentifiers: globalIdentifiers,
            versions: versions ?? SHAssetQuality.all,
            cacheHiResolution: cacheHiResolution
        ) { result in
            switch result {
            case .success(let dict):
                assets = dict
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

        return assets
    }
    
    /// Decrypt an asset version (quality) given its encrypted counterpart and its descriptor
    /// ```
    /// let localAssetsStore = SHLocalAssetStoreController(user: self.user)
    ///
    /// do {
    ///     let encryptedAsset = try localAssetsStore.encryptedAsset(
    ///         with: globalIdentifier,
    ///         filteringVersions: [quality]
    ///     )
    ///     let decryptedAsset = try localAssetsStore.decryptedAsset(
    ///         encryptedAsset: encryptedAsset,
    ///         descriptor: request.assetDescriptor,
    ///         quality: quality
    ///     )
    /// } catch {
    ///    // Handle
    /// }
    /// ```
    /// - Parameters:
    ///   - encryptedAsset: the encrypted asset
    ///   - descriptor: the asset descriptor. If none is provided, it's fetched from the local server
    ///   - quality: the version
    /// - Returns: the decrypted asset
    ///
    public func decryptedAsset(
        encryptedAsset: any SHEncryptedAsset,
        quality: SHAssetQuality,
        descriptor d: (any SHAssetDescriptor)? = nil
    ) throws -> any SHDecryptedAsset
    {
        let descriptor: any SHAssetDescriptor
        if d == nil {
            var dd: (any SHAssetDescriptor)? = nil
            let semaphore = DispatchSemaphore(value: 0)
            serverProxy.getLocalAssetDescriptors { result in
                if case .success(let descriptors) = result {
                    dd = descriptors.first(
                        where: { $0.globalIdentifier == encryptedAsset.globalIdentifier }
                    )
                }
                semaphore.signal()
            }
            let _ = semaphore.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
            guard dd != nil else {
                throw SHBackgroundOperationError.missingAssetInLocalServer(encryptedAsset.globalIdentifier)
            }
            descriptor = dd!
        } else {
            descriptor = d!
        }
        
        var sender: SHServerUser? = nil
        if descriptor.sharingInfo.sharedByUserIdentifier == self.user.identifier {
            sender = self.user
        } else {
            let users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: [descriptor.sharingInfo.sharedByUserIdentifier])
            guard users.count == 1, let serverUser = users.first,
                  serverUser.identifier == descriptor.sharingInfo.sharedByUserIdentifier
            else {
                throw SHBackgroundOperationError.unexpectedData(users)
            }
            sender = serverUser
        }
        
        return try self.user.decrypt(encryptedAsset, quality: quality, receivedFrom: sender!)
    }
    
    /// Returns a list of `SHAssetQuality` corresponding to the versions that are stored encrypted in the local asset store,
    /// for the asset having localIdentifier the identifier passed in.
    /// - Parameter localIdentifier: the asset local identifier
    /// - Returns: the list of versions (quality)
    public func locallyEncryptedVersions(
        forLocalIdentifier localIdentifier: String
    ) -> [SHAssetQuality] {
        var availableVersions = [SHAssetQuality]()
        let semaphore = DispatchSemaphore(value: 0)
        
        serverProxy.getLocalAssetDescriptors { descResult in
            if case .success(let descs) = descResult {
                if let descriptor = descs.first(where: {
                    descriptor in descriptor.localIdentifier == localIdentifier
                }) {
                    if let versions = self.locallyEncryptedVersions(
                        forGlobalIdentifiers: [descriptor.globalIdentifier]
                    ).values.first {
                        availableVersions = versions
                    }
                }
            }
            semaphore.signal()
        }
        
        let _ = semaphore.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        
        return availableVersions
    }
    
    /// Returns a list of `SHAssetQuality` corresponding to the versions that are stored encrypted in the local asset store,
    /// for the asset having localIdentifier the identifier passed in.
    /// - Parameter localIdentifier: the asset local identifier
    /// - Returns: the list of versions (quality)
    public func locallyEncryptedVersions(
        forGlobalIdentifiers globalIdentifiers: [String]
    ) -> [String: [SHAssetQuality]] {
        var availableVersions = [String: [SHAssetQuality]]()
        let semaphore = DispatchSemaphore(value: 0)
        
        serverProxy.getLocalAssets(
            withGlobalIdentifiers: globalIdentifiers,
            versions: SHAssetQuality.all,
            cacheHiResolution: false
        ) { assetResult in
            switch assetResult {
            case .success(let dict):
                for (globalId, encryptedAsset) in dict {
                    availableVersions[globalId] = Array(encryptedAsset.encryptedVersions.keys)
                }
            case .failure(_):
                break
            }
            semaphore.signal()
        }
        
        let _ = semaphore.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * 2))
        
        return availableVersions
    }
}

