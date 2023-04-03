//
//  KVStoreReadAPIs.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import Foundation

extension KBKVStore {
    
    /**
     Retrieves all keys and values in the KVS, asynchronously.
     
     - returns the dictionary containing all keys and values
     
     */
    @objc open func dictionaryRepresentation() async throws -> KBKVPairs {
        return try await self.backingStore.dictionaryRepresentation()
    }
    
    /**
     Retrieves all keys and values in the KVS, where the keys match the condition, asynchronously.
     
     - parameter condition: the condition to match keys against
     - returns the dictionary containing all keys and values matching the condition
     
     */
    @objc open func dictionaryRepresentation(forKeysMatching condition: KBGenericCondition) async throws -> KBKVPairs {
        return try await self.backingStore.dictionaryRepresentation(forKeysMatching: condition)
    }
    
    /**
     Retrieves all the keys in the KVS, asynchronously.
     
     - parameter completionHandler: the callback method
     - returns the keys
     
     */
    @objc open func keys() async throws -> [String] {
        return try await self.backingStore.keys()
    }
    
    /**
     Retrieves all the keys in the KVS matching the condition, asynchronously.
     
     - parameter condition: condition the keys need to satisfy
     - returns the keys
     
     */
    @objc open func keys(matching condition: KBGenericCondition) async throws -> [String] {
        return try await self.backingStore.keys(matching: condition)
    }
    
    /**
     Retrieves all the values in the KVS, asynchronously.
     
     - parameter completionHandler: the callback method
     - returns the values
     
     */
    @objc open func values() async throws -> [Any] {
        return try await self.backingStore.values()
    }
    
    /**
     Retrieves the value corresponding to the key in the KVS, asynchronously.
     
     - parameter key: the key
     - returns the value for the key. nil if the key doesn't exist, NSNull if set to null
     
     */
    open func value(for key: String) async throws -> Any? {
        return try await self.backingStore.value(for: key)
    }
    
    /**
     Retrieves the value corresponding to the keys passed as input from the KVS, asynchronously.
     Appends nil for values not present in the KVS for the corresponding key.
     
     - parameter keys: the set of keys
     - returns the list of values for the keys
     
     */
    open func values(for keys: [String]) async throws -> [Any] {
        var values = [Any]()
        for nullableValue in try await self.backingStore.values(for: keys) {
            if nullableValue == nil {
                values.append(NSNull())
            } else {
                values.append(nullableValue!)
            }
        }
        return values
    }
    
    /**
     Retrieves the values in the KVS whose keys pass the condition, asynchronously.
     
     - parameter condition: condition the keys need to satisfy
     - returns the list of values for the keys matching the condition
     
     */
    open func values(forKeysMatching condition: KBGenericCondition) async throws -> [Any?] {
        return try await self.backingStore.values(forKeysMatching: condition)
    }
}
