//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation

public let KnowledgeBaseXPCServiceBundleIdentifier = "com.gf.knowledgebase.storage.xpc"


#if !os(macOS)
// Use XPC only on macOS

class KBSQLXPCBackingStore : KBSQLBackingStore {
}

#else

class KBSQLXPCBackingStore : KBBackingStore {
    
    var name: String
    let connection: NSXPCConnection

    @objc required init(name: String) {
        self.name = name
        
            self.connection = NSXPCConnection(machServiceName: KnowledgeBaseXPCServiceBundleIdentifier,
                                              options: NSXPCConnection.Options(rawValue: 0))
            
            self.connection.remoteObjectInterface = KnowledgeBaseXPCUtils.KBServiceXPCInterface()
            
            self.connection.interruptionHandler = {
                log.info("XPC connection interrupted")
            }
            self.connection.invalidationHandler = {
                log.info("XPC connection invalidated")
            }
            
            self.connection.resume()
            
            log.info("XPC connection established (\(self.connection, privacy: .public))")
    }

    deinit {
        self.connection.invalidate()
    }

    class func mainInstance() -> Self {
        return self.init(name: KnowledgeBaseSQLDefaultIdentifier)
    }
    
    func xpcService() -> KBStorageXPCProtocol? {
        return self.connection.remoteObjectProxyWithErrorHandler { (error) in
            log.fault("XPC connection error: \(error.localizedDescription, privacy: .public)")
            } as? KBStorageXPCProtocol
    }
    
    
    //MARK: SELECT
    
    func keys(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        return service.keys(inStoreWithIdentifier: self.name) { error, keys in
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keys!))
            }
        }
    }
    
    func keys(matching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        service.keys(matching: condition, inStoreWithIdentifier: self.name) { error, keys in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keys!))
            }
        }
    }
    
    func value(for key: String, completionHandler: @escaping (Swift.Result<Any?, Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        service.value(forKey: key, inStoreWithIdentifier: self.name) {
            error, value in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(value))
            }
        }
    }
    
    func values(completionHandler: @escaping (Swift.Result<[Any], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.keysAndValues(inStoreWithIdentifier: self.name) { error, keysAndValues in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keysAndValues!.map { $1 }))
            }
        }
    }
    
    func values(for keys: [String], completionHandler: @escaping (Swift.Result<[Any?], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        var condition: KBGenericCondition? = nil
        for keyCondition in keys {
            let curr = KBGenericCondition(.equal, value: keyCondition)
            if let c = condition {
                condition = c.or(curr)
            } else {
                condition = curr
            }
        }
        
        service.keysAndValues(forKeysMatching: condition!, inStoreWithIdentifier: self.name) {
            error, keysAndValues in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keysAndValues!.map { $1 }))
            }
        }
    }
    
    func values(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[Any?], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.keysAndValues(forKeysMatching: condition, inStoreWithIdentifier: self.name) {
            error, keysAndValues in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keysAndValues!.map { $1 }))
            }
        }
    }
    
    func dictionaryRepresentation(completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.keysAndValues(inStoreWithIdentifier: self.name) {
            error, keysAndValues in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keysAndValues!))
            }
        }
    }
    
    func dictionaryRepresentation(forKeysMatching condition: KBGenericCondition,
                                  completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.keysAndValues(forKeysMatching: condition, inStoreWithIdentifier: self.name) {
            error, keysAndValues in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keysAndValues!))
            }
        }
    }
    
    func dictionaryRepresentation(createdWithin interval: DateInterval,
                                  limit: Int?,
                                  order: ComparisonResult,
                                  completionHandler: @escaping (Swift.Result<[Date: KBKVPairs], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.keysAndValues(createdWithin: interval, limit: limit ?? -1, order: order, inStoreWithIdentifier: self.name) {
            error, keysAndValuesByDate in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keysAndValuesByDate!))
            }
        }
    }
    
    func triplesComponents(matching condition: KBTripleCondition?,
                           completionHandler: @escaping (Swift.Result<[KBTriple], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.tripleComponents(matching: condition, inStoreWithIdentifier: self.name) {
            error, triples in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(triples!))
            }
        }
    }
    
    func verify(path: KBPath, completionHandler: @escaping (Swift.Result<Bool, Error>) -> ()) {
        log.error("path search in .SQL store not yet supported.") // TODO: Support
        completionHandler(.failure(KBError.notSupported))
    }

    //MARK: INSERT
    
    func set(value: Any?, for key: String, completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.save([key: value ?? NSNull()], toStoreWithIdentifier: self.name) {
            error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }

    func writeBatch() -> KBKVStoreWriteBatch {
        return KBSQLXPCWriteBatch(backingStore: self)
    }

    func setWeight(forLinkWithLabel predicate: String,
                   between subjectIdentifier: String,
                   and objectIdentifier: String,
                   toValue newValue: Int,
                   completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.setWeight(forLinkWithLabel: predicate,
                          between: subjectIdentifier,
                          and: objectIdentifier,
                          toValue: newValue,
                          inStoreWithIdentifier: self.name) {
            error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    func increaseWeight(forLinkWithLabel predicate: String,
                        between subjectIdentifier: String,
                        and objectIdentifier: String,
                        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.increaseWeight(forLinkWithLabel: predicate,
                               between: subjectIdentifier,
                               and: objectIdentifier,
                               inStoreWithIdentifier: self.name) {
            error, newWeight in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else if newWeight == kKBInvalidLinkWeight {
                completionHandler(.failure(KBError.unexpectedData(newWeight)))
            } else {
                completionHandler(.success(newWeight))
            }
        }
    }
    
    func decreaseWeight(forLinkWithLabel predicate: Label,
                        between subjectIdentifier: Label,
                        and objectIdentifier: Label,
                        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.decreaseWeight(forLinkWithLabel: predicate,
                               between: subjectIdentifier,
                               and: objectIdentifier,
                               inStoreWithIdentifier: self.name) {
            error, newWeight in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else if newWeight == kKBInvalidLinkWeight {
                completionHandler(.failure(KBError.unexpectedData(newWeight)))
            } else {
                completionHandler(.success(newWeight))
            }
        }
    }

    //MARK: DELETE
    
    func removeValue(for key: String, completionHandler: @escaping KBActionCompletion) {
        self.removeValues(for: [key], completionHandler: completionHandler)
    }
    
    func removeValues(for keys: [String], completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.removeValues(forKeys: keys, fromStoreWithIdentifier: self.name) { error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    func removeValues(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.removeValues(forKeysMatching: condition, fromStoreWithIdentifier: self.name) { error, keys in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keys ?? []))
            }
        }
    }
    
    func removeAll(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.removeAll(fromStoreWithIdentifier: self.name) { error, keys in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(keys ?? []))
            }
        }
    }
    
    func dropLink(withLabel predicate: String,
                  between subjectIdentifier: String,
                  and objectIdentifier: String,
                  completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.dropLink(withLabel: predicate,
                         between: subjectIdentifier,
                         and: objectIdentifier,
                         inStoreWithIdentifier: self.name) { error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    func dropLinks(withLabel predicate: String?,
                   from subjectIdentifier: String,
                   completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.dropLinks(withLabel: predicate,
                          from: subjectIdentifier,
                          inStoreWithIdentifier: self.name) { error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    func dropLinks(between subjectIdentifier: String,
                   and objectIdentifier: String,
                   completionHandler: @escaping KBActionCompletion) {
        guard let service = self.xpcService() else {
            completionHandler(.failure(KBError.fatalError("Could not connect to XPC service")))
            return
        }
        
        service.dropLinks(between: subjectIdentifier,
                          and: objectIdentifier,
                          inStoreWithIdentifier: self.name) { error in
            let _ = self // Retain self in the block to keep XPC connection alive
            if let error = error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    func disableSyncAndDeleteCloudData(completionHandler: @escaping KBActionCompletion) {
        completionHandler(.failure(KBError.notSupported))
    }
}

#endif
