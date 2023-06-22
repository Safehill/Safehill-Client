import Foundation
import KnowledgeBase

extension Array {
    func chunked(into size: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0 ..< Swift.min($0 + size, count)])
        }
    }
}

// MARK: - LocalServer Extension

extension LocalServer {
    
    func moveDataToNewKeyFormat(for dictionary: [String: Any]) throws -> Bool {
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
    
    public func runDataMigrations(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        dispatchPrecondition(condition: .notOnQueue(DispatchQueue.main))
        
        /// Remove KnowledgeGraph entries at launch
        do {
            let _ = try SHShareGraph.sharedInstance.store.removeAll()
        } catch {
            log.warning("Failed to remove deprecated data from the KnowledgeGraph")
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
        
        guard errors.count == 0 else {
            completionHandler(.failure(errors.first!))
            return
        }
        
        // Hi Res migrations
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
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * assetIdentifiers.count))
        guard dispatchResult == .success else {
            completionHandler(.failure(SHBackgroundOperationError.timedOut))
            return
        }
        guard errors.count == 0 else {
            completionHandler(.failure(errors.first!))
            return
        }
        completionHandler(.success(()))
    }
}
