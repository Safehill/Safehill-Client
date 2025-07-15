import Foundation

let AssetDownloadPipelineProcessor = SHBackgroundOperationProcessor<SHRemoteDownloadOperation>()
let AssetSyncPipelineProcessor = SHBackgroundOperationProcessor<SHAssetsSyncOperation>()

extension SHGlobalSyncOperation {
    
    public func syncAllAssets(
        qos: DispatchQoS.QoSClass = .default
    ) async throws {
        let assetsDownloadOperation = SHRemoteDownloadOperation(
            user: self.user,
            assetSyncingDelegates: self.assetSyncingDelegates,
            activitySyncingDelegates: self.activitySyncingDelegates,
            photoIndexer: SHPhotosIndexer()
        )
        let assetsSyncOperation = SHAssetsSyncOperation(
            user: self.user,
            assetsDelegates: self.assetSyncingDelegates
        )
        
        await AssetDownloadPipelineProcessor.runOperation(
            assetsDownloadOperation,
            qos: qos
        ) { _ in }
        
        await AssetSyncPipelineProcessor.runOperation(
            assetsSyncOperation,
            qos: qos
        ) { _ in }
    }
    
    public func syncAssets(
        with globalIdentifiers: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass = .default,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let assetsDownloadOperation = SHRemoteDownloadOperation(
            user: user,
            assetSyncingDelegates: self.assetSyncingDelegates,
            activitySyncingDelegates: self.activitySyncingDelegates,
            photoIndexer: SHPhotosIndexer()
        )
        let assetsSyncOperation = SHAssetsSyncOperation(
            user: self.user,
            assetsDelegates: self.assetSyncingDelegates
        )
        
        assetsDownloadOperation.run(
            for: globalIdentifiers,
            filteringGroups: nil,
            startingFrom: nil,
            qos: qos
        ) { res1 in
            if case .failure(let failure) = res1 {
                completionHandler(.failure(failure))
                return
            }
            assetsSyncOperation.run(
                qos: qos,
                for: globalIdentifiers
            ) { res2 in
                completionHandler(res2)
            }
        }
    }
    
    public func syncGroup(
        with groupId: String,
        qos: DispatchQoS.QoSClass = .default,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let assetsDownloadOperation = SHRemoteDownloadOperation(
            user: user,
            assetSyncingDelegates: self.assetSyncingDelegates,
            activitySyncingDelegates: self.activitySyncingDelegates,
            photoIndexer: SHPhotosIndexer()
        )
        let assetsSyncOperation = SHAssetsSyncOperation(
            user: self.user,
            assetsDelegates: self.assetSyncingDelegates
        )
        
        assetsDownloadOperation.run(
            for: nil,                   /// All assets
            filteringGroups: [groupId], /// in group `groupId`
            startingFrom: .distantPast,
            qos: qos
        ) { result in
            switch result {
            case .failure(let failure):
                completionHandler(.failure(failure))
                return
            case .success(let assetsAndDescriptors):
                assetsSyncOperation.run(
                    qos: qos,
                    for: Array(assetsAndDescriptors.keys)
                ) { res2 in
                    completionHandler(res2)
                }
            }
        }
    }
}
