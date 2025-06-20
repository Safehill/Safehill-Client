import Foundation

extension SHGlobalSyncOperation {
    
    public func syncAllAssets() async throws {
        let assetsDownloadOperation = SHRemoteDownloadOperation(
            user: self.user,
            assetSyncingDelegates: self.assetSyncingDelegates,
            restorationDelegate: self.restorationDelegate,
            photoIndexer: SHPhotosIndexer()
        )
        let assetsSyncOperation = SHAssetsSyncOperation(
            user: self.user,
            assetsDelegates: self.assetSyncingDelegates
        )
        
        try await withUnsafeThrowingContinuation { continuation in
            assetsDownloadOperation.run(qos: .default) {
                rdor in
                assetsSyncOperation.run(qos: .default) {
                    asor in
                    switch (rdor, asor) {
                    case (.success, .success):
                        self.log.debug("assets sync completed")
                        continuation.resume()
                    case (.failure(let e1), _):
                        self.log.debug("assets sync failed")
                        continuation.resume(throwing: e1)
                    case (_, .failure(let e2)):
                        self.log.debug("assets sync failed")
                        continuation.resume(throwing: e2)
                    }
                }
            }
        }
    }
    
    public func syncAssets(
        with globalIdentifiers: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass = .default,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let assetsDownloadOperation = SHRemoteDownloadOperation(
            user: user,
            assetSyncingDelegates: self.assetSyncingDelegates,
            restorationDelegate: self.restorationDelegate,
            photoIndexer: SHPhotosIndexer()
        )
        let assetsSyncOperation = SHAssetsSyncOperation(
            user: self.user,
            assetsDelegates: self.assetSyncingDelegates
        )
        
        assetsDownloadOperation.run(for: globalIdentifiers,
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
        SHRemoteDownloadOperation(
            user: user,
            assetSyncingDelegates: self.assetSyncingDelegates,
            restorationDelegate: self.restorationDelegate,
            photoIndexer: SHPhotosIndexer()
        )
        .run(
            for: nil,                   /// All assets
            filteringGroups: [groupId], /// in group `groupId`
            startingFrom: .distantPast,
            qos: .userInitiated
        ) { result in
            switch result {
            case .failure(let failure):
                completionHandler(.failure(failure))
                return
            case .success(let assetsAndDescriptors):
                SHAssetsSyncOperation(
                    user: self.user,
                    assetsDelegates: self.assetSyncingDelegates
                ).run(
                    qos: qos,
                    for: Array(assetsAndDescriptors.keys)
                ) { res2 in
                    completionHandler(res2)
                }
            }
        }
    }
}
