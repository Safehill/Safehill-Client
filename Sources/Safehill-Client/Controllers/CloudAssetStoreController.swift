import Foundation

public typealias GlobalIdentifier = String


public enum SHAssetStoreError: Error, LocalizedError {
    case notImplemented
    case failedToCreateRemoteAsset
    
    public var errorDescription: String? {
        switch self {
        case .notImplemented:
            return "Not implemented"
        case .failedToCreateRemoteAsset:
            return "Failed to create remote asset"
        }
    }
}

struct SHAssetStoreController {
    
    public let user: SHLocalUser
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    func upload(asset encryptedAsset: any SHEncryptedAsset,
                with groupId: String,
                filterVersions: [SHAssetQuality]? = nil) throws -> SHServerAsset
    {
        let serverAsset = try self.createRemoteAsset(
            encryptedAsset,
            groupId: groupId,
            filterVersions: filterVersions
        )
        
        do {
            try self.upload(serverAsset: serverAsset, asset: encryptedAsset, filterVersions: filterVersions)
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
    
    private func createRemoteAsset(_ asset: any SHEncryptedAsset,
                                   groupId: String,
                                   filterVersions: [SHAssetQuality]?) throws -> SHServerAsset {
        log.info("creating asset \(asset.globalIdentifier) on server")
        
        var serverAsset: SHServerAsset? = nil
        var error: Error? = nil
        
        let group = DispatchGroup()
        
        group.enter()
        self.serverProxy.createRemoteAssets([asset], groupId: groupId, filterVersions: filterVersions) { result in
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
        self.serverProxy.remoteServer.deleteAssets(withGlobalIdentifiers: [globalIdentifier]) { result in
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
    
    private func upload(serverAsset: SHServerAsset,
                        asset: any SHEncryptedAsset,
                        filterVersions: [SHAssetQuality]?) throws {
        log.info("uploading asset \(asset.globalIdentifier) to the CDN")
        
        var error: Error? = nil
        
        let group = DispatchGroup()
        group.enter()
        self.serverProxy.upload(serverAsset: serverAsset, asset: asset, filterVersions: filterVersions) { result in
            if case .failure(let err) = result {
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHUploadTimeoutInMilliseconds))
        
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        
        if let error = error {
            throw error
        }
    }
}
