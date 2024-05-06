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
    
    func moveDataToNewKeyFormat(for dictionary: [String: Any]) throws -> Bool {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            throw KBError.databaseNotReady
        }
        
        var pre_1_4_keys = [String]()
        let writeBatch = assetStore.writeBatch()
        
        for (key, value) in dictionary {
            guard key.prefix(6) != "data::" else {
                /// Skip the new formats
                continue
            }
            guard let value = value as? [String: Any] else {
                /// Skip the unreadable values
                continue
            }
            
            ///
            /// More than 2 keys, encrypted data key present -> it's a pre 1.4 release
            /// If there's no new format `data::<quality>::<globalId>` then migrate to 1.4+ format
            ///
            if value.keys.count > 2 && value["encryptedData"] != nil {
                var metadataValue = value
                metadataValue.removeValue(forKey: "encryptedData")
                let dataValue = [
                    "assetIdentifier": value["assetIdentifier"],
                    "encryptedData": value["encryptedData"]
                ]
                writeBatch.set(value: dataValue, for: "data::" + key)
                writeBatch.set(value: metadataValue, for: key)
                
                pre_1_4_keys.append(key)
            }
        }
        
        guard pre_1_4_keys.count > 0 else {
            return false
        }
        
        try writeBatch.write()
        
        var condition = KBGenericCondition(value: false)
        for key in pre_1_4_keys {
            condition = condition.or(KBGenericCondition(.equal, value: key))
        }
        let removed = try assetStore.removeValues(forKeysMatching: condition)
        log.info("Migrated \(removed.count) keys")
        return removed.count > 0
    }
    
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
                        var knownUsersInBatch = [UserIdentifier: Bool]()
                        for senderId in sendersInBatch {
                            knownUsersInBatch[senderId] = try SHKGQuery.isUserKnown(withIdentifier: senderId, by: self.requestor.identifier)
                        }
                        
                        ///
                        /// Additions and Edits
                        ///
                        for remoteDescriptor in relevantRemoteDescriptors {
                            let sender = remoteDescriptor.sharingInfo.sharedByUserIdentifier
                            
                            guard (knownUsersInBatch[sender] ?? false) == true else {
                                continue
                            }
                            
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
    
    /// Run migration of data in local databases
    /// - Parameters:
    ///   - currentBuild: the current client build, if available
    ///   - completionHandler: the callback
    public func runDataMigrations(
        currentBuild: Int?,
        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()
    ) {
        dispatchPrecondition(condition: .notOnQueue(DispatchQueue.main))
        
        ///
        /// Data used to be stored along with the descriptor in the local store, which is inefficient. Translate them to the new format
        ///
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            log.warning("failed to connect to the local asset store")
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var assetIdentifiers = Set<String>()
        
        // Low Res migrations
        var condition = KBGenericCondition(.beginsWith, value: "low::")
        
        let group = DispatchGroup()
        var errors = [Error]()
        
        group.enter()
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Swift.Result) in
            switch result {
            case .success(let keyValues):
                for k in keyValues.keys {
                    if let range = k.range(of: "low::") {
                        let globalIdentifier = "" + k[range.upperBound...]
                        assetIdentifiers.insert(globalIdentifier)
                    }
                }
                do {
                    let _ = try self.moveDataToNewKeyFormat(for: keyValues)
                } catch {
                    log.warning("Failed to migrate data format for asset keys \(keyValues.keys)")
                    errors.append(error)
                }
            case .failure(let error):
                errors.append(error)
            }
            group.leave()
        }
        
        group.notify(queue: .global()) {
            guard errors.count == 0 else {
                completionHandler(.failure(errors.first!))
                return
            }
            
            /// Hi Res migrations
            condition = KBGenericCondition(.beginsWith, value: "hi::")

            for chunk in Array(assetIdentifiers).chunked(into: 10) {
                var assetCondition = KBGenericCondition(value: false)
                for assetIdentifier in chunk {
                    assetCondition = assetCondition.or(KBGenericCondition(.endsWith, value: assetIdentifier))
                }
                condition = condition.and(assetCondition)
                
                group.enter()
                assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Swift.Result) in
                    switch result {
                    case .success(let keyValues):
                        do {
                            if keyValues.count > 0 {
                                let _ = try self.moveDataToNewKeyFormat(for: keyValues)
                            }
                        } catch {
                            log.warning("Failed to migrate data format for asset keys \(keyValues.keys)")
                            errors.append(error)
                        }
                    case .failure(let error):
                        errors.append(error)
                    }
                    group.leave()
                }
            }
            
            group.notify(queue: .global()) {
                guard errors.count == 0 else {
                    completionHandler(.failure(errors.first!))
                    return
                }
                
                completionHandler(.success(()))
            }
        }
    }
}
