//
//  SyncStoreReadAPI.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/20/21.
//

import Foundation

extension KBKVStore {
    
    /**
     Retrieves all keys and values in the KVS.
     Blocking version.
     
     - returns: the Dictionary representation of all values in the KVS.
     
     */
    @objc open func dictionaryRepresentation() throws -> KBKVPairs {
        return try self.backingStore.dictionaryRepresentation()
    }
    
    /**
     Retrieves all keys and values in the KVS, where the keys match the condition.
     Blocking version.
     
     - parameter condition: the condition to match keys against
     - returns: the Dictionary representation of all values in the KVS.
     
     */
    @objc open func dictionaryRepresentation(forKeysMatching condition: KBGenericCondition) throws -> KBKVPairs {
        return try self.backingStore.dictionaryRepresentation(forKeysMatching: condition)
    }
    
    /**
     Retrieves all the keys in the KVS.
     Blocking version.
     
     - returns the keys
     
     */
    @objc open func keys() throws -> [String] {
        return try self.backingStore.keys()
    }
    
    /**
     Retrieves all the keys in the KVS matching the condition.
     Blocking version.
     
     - parameter condition: condition the keys need to satisfy
     - returns the keys
     
     */
    @objc open func keys(matching condition: KBGenericCondition) throws -> [String] {
        return try self.backingStore.keys(matching: condition)
    }
    
    /**
     Retrieves all the values in the KVS.
     Blocking version.
     
     - returns the keys
     
     */
    @objc open func values() throws -> [Any] {
        return try self.backingStore.values()
    }
    
    /**
     Retrieves the value corresponding to the key in the KVS.
     Blocking version.
     
     - parameter: key the key
     
     - returns: the value
     
     */
    open func value(for key: String) throws -> Any? {
        return try self.backingStore.value(for: key)
    }
    
    /**
     Retrieves the value corresponding to the keys passed as input from the KVS.
     Appends nil for values not present in the KVS for the corresponding key.
     Blocking version.
     
     - parameter keys: the set of keys
     
     - returns: the values
     
     */
    open func values(for keys: [String]) throws -> [Any?] {
        return try self.backingStore.values(for: keys)
    }
    
    /**
     Retrieves the values in the KVS whose keys pass the condition.
     Blocking version.
     
     - parameter condition: condition the keys need to satisfy
     
     - returns: the values
     
     */
    open func values(forKeysMatching condition: KBGenericCondition) throws -> [Any?] {
        return try self.backingStore.values(forKeysMatching: condition)
    }
}

extension KBKnowledgeStore {
    
    @objc open func entities() throws -> [KBEntity] {
        return try KBSyncMethodReturningInitiable(execute: self.entities)
    }
    
    /**
     Matches triples need against the condition passed as argument
     
     - parameter condition: matches only triples having satisfying this condition.
     If nil, matches all triples
     
     - returns: The array of triples in a dictionary with keys: subject, predicate, object
     */
    @objc open func triples(matching condition: KBTripleCondition?) throws -> [KBTriple] {
        return try self.backingStore.triplesComponents(matching: condition)
    }
}
