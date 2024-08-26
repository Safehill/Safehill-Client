import Foundation
import KnowledgeBase

public extension Array {
    func chunked(into size: Int) -> [[Element]] {
        guard !isEmpty && size > 0 else {
            return []
        }
        
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0 ..< Swift.min($0 + size, count)])
        }
    }
    
    func chunkedWithLinearDecrease() -> [[Element]] {
        guard !isEmpty else {
            return []
        }
        
        let totalElements = count
        var chunks: [[Element]] = []
        var ratio = 0.5
        var currentChunkSize = Swift.max(1, Int(Double(totalElements) * ratio))
        
        var currentIndex = 0
        while currentIndex < totalElements {
            let endIndex = Swift.min(currentIndex + currentChunkSize, totalElements)
            let chunk = Array(self[currentIndex..<endIndex])
            chunks.append(chunk)
            
            currentIndex += currentChunkSize
            let remainingElements = totalElements - currentIndex
            ratio *= 1.5
            currentChunkSize = Swift.max(1, Int(Double(remainingElements) * ratio))
        }
        
        return chunks
    }
}

// MARK: - LocalServer Extension

extension LocalServer {
    
    public func syncLocalGraphWithServer(
        dryRun: Bool = true,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.requestor.serverProxy.getRemoteAssetDescriptors(after: nil) { remoteResult in
            switch remoteResult {
            case .success(let remoteDescriptors):
                var uniqueAssetGids = Set<GlobalIdentifier>()
                for remoteDescriptor in remoteDescriptors {
                    uniqueAssetGids.insert(remoteDescriptor.globalIdentifier)
                }
                
                do {
                    for uniqueAssetGidsChunk in Array(uniqueAssetGids).chunked(into: 30) {
                        let assetToUsers: [GlobalIdentifier: [(SHKGPredicate, UserIdentifier)]]
                        assetToUsers = try SHKGQuery.usersConnectedTo(assets: Array(uniqueAssetGidsChunk))
                        
                        let relevantRemoteDescriptors = remoteDescriptors.filter({ uniqueAssetGidsChunk.contains($0.globalIdentifier) })
                        
                        let sendersInBatch = Array(Set(relevantRemoteDescriptors.map({ $0.sharingInfo.sharedByUserIdentifier })))
                        
                        ///
                        /// Additions and Edits
                        ///
                        for remoteDescriptor in relevantRemoteDescriptors {
                            let sender = remoteDescriptor.sharingInfo.sharedByUserIdentifier
                            
                            let assetGid = remoteDescriptor.globalIdentifier
                            
                            if assetToUsers[assetGid] == nil {
                                let recipients = Array(remoteDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys)
                                log.info("[graph-sync] missing triples from <asset:\(assetGid)> <from:\(sender)> <to:\(recipients)>")
                                if dryRun == false {
                                    try SHKGQuery.ingestShare(
                                        of: assetGid,
                                        from: sender,
                                        to: recipients
                                    )
                                }
                            } else {
                                // TODO: Edits
                            }
                        }
                        
                        usleep(useconds_t(10 * 1000)) // sleep 10ms
                    }
                    
                    ///
                    /// Removals
                    ///
                    // TODO: Removals
                    
                    let extraAssetIdsInGraph = try SHKGQuery.assetGlobalIdentifiers(notIn: Array(uniqueAssetGids))
                    log.info("[graph-sync] extra assets in graph \(extraAssetIdsInGraph)")
                    if dryRun == false {
                        try SHKGQuery.removeAssets(with: extraAssetIdsInGraph)
                    }
                    
                    completionHandler(.success(()))
                }
                catch {
                    completionHandler(.failure(error))
                    return
                }
            case .failure(let err):
                log.info("[graph-sync] failed to sync with remote server: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
    
    ///
    /// Data used to be stored along with the descriptor in the local store, which is inefficient. Translate them to the new format
    ///
    private func runAssetDataMigration(
        currentBuild: Int?,
        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()
    ) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            log.warning("failed to connect to the local asset store")
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        completionHandler(.success(()))
    }
    
    ///
    /// Photo messages in threads used to be cached under the following keys
    /// `user-thread::<thread_id>::assets`, `user-thread::<thread_id>::photoMessages` and `user-thread::<thread_id>::nonPhotoMessages`
    /// In recent versions they are cached under keys `user-thread::<thread_id>::assets::photoMessage`,
    /// alongside `user-thread::<thread_id>::assets::nonPhotoMessage`.
    /// Simply remove the old cache under the old keys.
    ///
    func runAssetThreadsMigration(
        currentBuild: Int?,
        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            log.warning("failed to connect to the local user store")
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let condition = KBGenericCondition(
            .beginsWith,
            value: SHInteractionAnchor.thread.rawValue + "::"
        ).and(
            KBGenericCondition(
                .endsWith,
                value: "::assets"
            ).or(KBGenericCondition(
                .endsWith,
                value: "::photoMessages"
            )).or(KBGenericCondition(
                .endsWith,
                value: "::nonPhotoMessages"
            ))
        )
        
        userStore.removeValues(forKeysMatching: condition) { result in
            switch result {
            case .success:
                completionHandler(.success(()))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
        
    
    /// Run migration of data in local databases
    /// - Parameters:
    ///   - currentBuild: the current client build, if available
    ///   - completionHandler: the callback
    public func runDataMigrations(
        currentBuild: Int?,
        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()
    ) {
        dispatchPrecondition(condition: .notOnQueue(DispatchQueue.main))
        
        var errors = [Error]()
        
        let group = DispatchGroup()
        
        group.enter()
        self.runAssetDataMigration(currentBuild: currentBuild) { result1 in
            if case .failure(let error) = result1 {
                errors.append(error)
            }
            
            self.runAssetThreadsMigration(currentBuild: currentBuild) { result2 in
                if case .failure(let error) = result2 {
                    errors.append(error)
                }
                
                group.leave()
            }
        }
        
        group.notify(queue: .global()) {
            guard errors.isEmpty == true else {
                completionHandler(.failure(errors.first!))
                return
            }
            
            completionHandler(.success(()))
        }
    }
}
