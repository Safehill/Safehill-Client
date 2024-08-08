import Foundation

public typealias GlobalIdentifier = String


public enum SHAssetStoreError: Error, LocalizedError {
    case notImplemented
    case noEntries
    case failedToCreateRemoteAsset
    case failedToRetrieveLocalAsset
    case invalidRequest(String)
    case failedToUnshareSomeAssets
    
    public var errorDescription: String? {
        switch self {
        case .notImplemented:
            return "Not implemented"
        case .noEntries:
            return "Could not find an entry for the requested asset(s)"
        case .failedToCreateRemoteAsset:
            return "Failed to create remote asset"
        case .failedToRetrieveLocalAsset:
            return "Failed to retrieve asset from cache"
        case .invalidRequest(let reason):
            return "Invalid request: \(reason)"
        case .failedToUnshareSomeAssets:
            return "Some shares could not be reverted"
        }
    }
}

struct SHAssetStoreController {
    
    public let user: SHAuthenticatedLocalUser
    
    func upload(
        asset encryptedAsset: any SHEncryptedAsset,
        with groupId: String,
        filterVersions: [SHAssetQuality]? = nil,
        force: Bool
    ) async throws -> SHServerAsset
    {
        let serverAsset = try self.createRemoteAsset(
            encryptedAsset,
            groupId: groupId,
            filterVersions: filterVersions,
            force: force
        )
        
        do {
            try await self.upload(serverAsset: serverAsset, asset: encryptedAsset, filterVersions: filterVersions)
        } catch {
            try? self.deleteRemoteAsset(globalIdentifier: encryptedAsset.globalIdentifier)
            throw error
        }
        
        return serverAsset
    }
    
    func download(globalIdentifiers: [GlobalIdentifier],
                  filterVersions: [SHAssetQuality]? = nil) throws -> [any SHDecryptedAsset] {
        throw SHAssetStoreError.notImplemented
    }
}

extension SHAssetStoreController {
    
    private func createRemoteAsset(
        _ asset: any SHEncryptedAsset,
        groupId: String,
        filterVersions: [SHAssetQuality]?,
        force: Bool
    ) throws -> SHServerAsset {
        log.info("creating asset \(asset.globalIdentifier) on server")
        
        var serverAsset: SHServerAsset? = nil
        var error: Error? = nil
        
        let group = DispatchGroup()
        
        group.enter()
        self.user.serverProxy.remoteServer.create(
            assets: [asset],
            groupId: groupId,
            filterVersions: filterVersions,
            force: force
        ) { result in
            switch result {
            case .success(let serverAssets):
                if serverAssets.count == 1 {
                    serverAsset = serverAssets.first!
                } else {
                    error = SHBackgroundOperationError.unexpectedData(serverAssets)
                }
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

        return serverAsset!
    }
    
    private func deleteRemoteAsset(globalIdentifier: String) throws {
        log.info("deleting asset \(globalIdentifier) from server")
        
        let group = DispatchGroup()
        group.enter()
        self.user.serverProxy.remoteServer.deleteAssets(
            withGlobalIdentifiers: [globalIdentifier]
        ) { result in
            if case .failure(let err) = result {
                log.info("failed to remove asset \(globalIdentifier) from server: \(err.localizedDescription)")
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
    private func upload(
        serverAsset: SHServerAsset,
        asset: any SHEncryptedAsset,
        filterVersions: [SHAssetQuality]?
    ) async throws {
        log.info("uploading asset \(asset.globalIdentifier) to the CDN")
        
        try await self.user.serverProxy.upload(
            serverAsset: serverAsset,
            asset: asset,
            filterVersions: filterVersions
        )
    }
}
