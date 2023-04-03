//
//  SynchedSQL.swift
//
//
//  Created by Gennaro Frazzingaro on 6/28/21.
//

import Foundation

#if !os(macOS)
// Use XPC only on macOS

class KBCloudKitSQLXPCBackingStore : KBCloudKitSQLBackingStore {
}

#else

class KBCloudKitSQLXPCBackingStore : KBSQLXPCBackingStore {
    
    override var name: String {
        get { return KnowledgeBaseSQLSynchedIdentifier }
        set(v) {}
    }
    
    class override func mainInstance() -> Self {
        return self.init(name: KnowledgeBaseSQLSynchedIdentifier)
    }
    
    //MARK: INSERT
    
    override func set(value: Any?, for key: String, completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.save([key: value ?? NSNull()],
                     toSynchedStoreWithIdentifier: self.name) { error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    override func writeBatch() -> KBKVStoreWriteBatch {
        return KBCloudKitSQLXPCWriteBatch(backingStore: self)
    }
    
    //MARK: DELETE
    
    override func removeValues(for keys: [String], completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.removeValues(forKeys: keys,
                             fromSynchedStoreWithIdentifier: self.name) { error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    override func removeValues(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.removeValues(forKeysMatching: condition, fromSynchedStoreWithIdentifier: self.name) { error, keys in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keys ?? []))
            }
        }
    }
    
    override func removeAll(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.removeAll(fromSynchedStoreWithIdentifier: self.name) { error, keys in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keys ?? []))
            }
        }
    }
    
    override func disableSyncAndDeleteCloudData(completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.disableSyncAndDeleteCloudData() { error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
}

#endif
