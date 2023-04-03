//
//  SyncStoreWriteAPI.swift
//
//
//  Created by Gennaro Frazzingaro on 7/20/21.
//

import Foundation

public extension KBKVStoreWriteBatch {
    func write() throws {
        try KBSyncMethodReturningVoid(execute: self.write(completionHandler:))
    }
}

extension KBKVStore {
    /**
     Assign a value, or nil, to a specific key in the KVS.
     Blocking version.
     
     - parameter value: the value
     - parameter key: the key
     */
    public func set(value: Any?, for key: String) throws {
        guard self.supportsSecureCoding(value) else {
            log.error("Won't save a non NSSecureCoding compliant value (\(String(describing: value)) for key (\(key))")
            return
        }
        
        let writeBatch = self.writeBatch()
        writeBatch.set(value: value, for: key)
        log.debug("setting value \(String(describing: value)) for key (\(key))")
        try KBSyncMethodReturningVoid(execute:writeBatch.write)
        self.delegate?.kvDataDidChange(addedKeys: [key], removedKeys: [])
    }
    
    /**
     Remove a tuple in the KVS, given its key.
     Blocking version.
     
     - parameter key: the key
     */
    @objc public func removeValue(for key: String) throws {
        try KBSyncMethodReturningVoid { c in
            self.removeValue(for: key, completionHandler: c)
        }
    }
    
    /**
     Remove a set of tuples in the KVS, given their keys.
     Blocking version
     
     - parameter keys: the keys
     */
    @objc public func removeValues(for keys: [String]) throws {
        try KBSyncMethodReturningVoid { c in
            self.removeValues(for: keys, completionHandler: c)
        }
    }
    
    /**
     Remove a set of tuples in the KVS, matching the condition.
     Blocking version
     
     - parameter condition: the condition
     */
    @objc public func removeValues(forKeysMatching condition: KBGenericCondition) throws -> [String] {
        try KBSyncMethodReturningInitiable { c in
            self.removeValues(forKeysMatching: condition, completionHandler: c)
        }
    }
    
    /**
     Remove all values in the KVS
     */
    @objc public func removeAll() throws -> [String] {
        try KBSyncMethodReturningInitiable(execute: self.removeAll)
    }
}

extension KBKnowledgeStore {
    
    /**
     Remove the entity with a certain identifier.
     WARN: This will remove all the connections to this entity in the graph.
     Blocking version.
     
     - parameter identifier: the identifier
     */
    @objc public func removeEntity(_ identifier: Label) throws {
        try KBSyncMethodReturningVoid { c in
            self.removeEntity(identifier, completionHandler: c)
        }
    }
}
