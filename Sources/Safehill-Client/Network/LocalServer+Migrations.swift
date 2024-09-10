import Foundation
import KnowledgeBase

let kSHDBMigrationsUDStoreName = "com.gf.safehill.snoog.migrations"
let kSHLastBuildMigratedToKey = "lastBuildMigratedTo"

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
    
    ///
    /// Data used to be stored along with the descriptor in the local store, which is inefficient. Translate them to the new format
    ///
    private func runAssetDataMigration(
        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()
    ) {
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
    
    func runVersionMigration(
        to: Int,
        from: Int?,
        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            log.warning("failed to connect to the local user store")
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        if to == 13 { // 1.3.0 migrations
            let condition = KBGenericCondition(
                .beginsWith,
                value: "invitations::"
            )
            
            userStore.removeValues(forKeysMatching: condition) { result in
                switch result {
                case .success:
                    completionHandler(.success(()))
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        } else {
            completionHandler(.success(()))
        }
    }
    
    /// Run migration of data in local databases
    /// - Parameters:
    ///   - currentBuild: the current client build, if available
    ///   - completionHandler: the callback
    public func runDataMigrations(
        currentBuild: String?,
        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()
    ) {
        dispatchPrecondition(condition: .notOnQueue(DispatchQueue.main))
        
        var errors = [Error]()
        
        let group = DispatchGroup()
        
        group.enter()
        self.runAssetDataMigration { result1 in
            if case .failure(let error) = result1 {
                errors.append(error)
            }
            
            self.runAssetThreadsMigration { result2 in
                if case .failure(let error) = result2 {
                    errors.append(error)
                }
                
                group.leave()
            }
        }
        
        if let currentBuild,
           let currentNumericVersion = Int(currentBuild.prefix(3).split(separator: ".").joined()) {
            
            let migrationUserDefaults = UserDefaults(suiteName: kSHDBMigrationsUDStoreName)!
            let lastMigrationRunForVersion = migrationUserDefaults.value(forKey: kSHLastBuildMigratedToKey) as? String
            
            let lastRunNumericVersion: Int?
            if let lastMigrationRunForVersion {
                lastRunNumericVersion = Int(lastMigrationRunForVersion.prefix(3).split(separator: ".").joined())
            } else {
                lastRunNumericVersion = nil
            }
        
            if lastRunNumericVersion == nil || currentNumericVersion > lastRunNumericVersion! {
                
                group.enter()
                
                self.runVersionMigration(to: currentNumericVersion, from: lastRunNumericVersion) { result3 in
                    if case .failure(let error) = result3 {
                        errors.append(error)
                    } else {
                        migrationUserDefaults.set(currentBuild, forKey: kSHLastBuildMigratedToKey)
                    }
                    
                    group.leave()
                }
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
