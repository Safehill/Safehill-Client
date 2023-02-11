import Foundation

public typealias LocalIdentifier = String


public struct SHLocalAssetStoreController {
    public let user: SHLocalUser
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func encryptedAsset(
        with globalIdentifier: GlobalIdentifier,
        versions: [SHAssetQuality]? = nil
    ) throws -> any SHEncryptedAsset
    {
        let result = try self.encryptedAssets(with: [globalIdentifier],
                                              versions: versions)
        guard let asset = result[globalIdentifier] else {
            throw SHBackgroundOperationError.unexpectedData(result)
        }
        return asset
    }
    
    public func encryptedAssets(
        with globalIdentifiers: [GlobalIdentifier],
        versions: [SHAssetQuality]? = nil
    ) throws -> [GlobalIdentifier: any SHEncryptedAsset]
    {
        var assets = [GlobalIdentifier: any SHEncryptedAsset]()
        var error: Error? = nil
        
        let group = DispatchGroup()
        group.enter()
        self.serverProxy.getLocalAssets(
            withGlobalIdentifiers: globalIdentifiers,
            versions: versions ?? SHAssetQuality.all
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
    ///   - descriptor: the asset descriptor
    ///   - quality: the version
    /// - Returns: the decrypted asset
    ///
    public func decryptedAsset(
        encryptedAsset: any SHEncryptedAsset,
        descriptor: any SHAssetDescriptor,
        quality: SHAssetQuality
    ) throws -> any SHDecryptedAsset
    {
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
}

