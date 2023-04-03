//
//  KVStoreReadAPIs.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/17/21.
//


import Foundation

func KBObjectiveCAPIResultReturningInitiable<T: Initiable>(completionHandler: @escaping (Error?, T) -> (), _ f: @escaping (@escaping (Swift.Result<T, Error>) -> ()) -> ()) {
    f { result in
        switch result {
        case .success(let res): completionHandler(nil, res)
        case .failure(let err): completionHandler(err, T.init())
        }
    }
}

// MARK: - KBKVStore Read API

extension KBKVStore {
    
    /**
     Retrieves all keys and values in the KVS.
     
     - parameter completionHandler: the callback method
     
     */
    open func dictionaryRepresentation(completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ()) {
        return self.backingStore.dictionaryRepresentation(completionHandler: completionHandler)
    }
    @objc open func dictionaryRepresentation(completionHandler: @escaping (Error?, KBKVPairs) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler, self.dictionaryRepresentation)
    }
    
    /**
     Retrieves all keys and values in the KVS, where the keys match the condition.
     
     - parameter condition: the condition to match keys against
     - parameter completionHandler: the callback method
     
     */
    open func dictionaryRepresentation(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ()) {
        
        return self.backingStore.dictionaryRepresentation(forKeysMatching: condition, completionHandler: completionHandler)
    }
    @objc open func dictionaryRepresentation(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Error?, KBKVPairs) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler, self.dictionaryRepresentation)
    }
    
    
    /**
     Retrieves all the keys in the KVS.
     
     - parameter completionHandler: the callback method
     
     */
    open func keys(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        return self.backingStore.keys(completionHandler: completionHandler)
    }
    @objc open func keys(completionHandler: @escaping (Error?, [String]) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler, self.keys)
    }
    
    /**
     Retrieves all the keys in the KVS matching the condition.
     
     - parameter condition: condition the keys need to satisfy
     - parameter completionHandler: the callback method
     
     */
    open func keys(matching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        return self.backingStore.keys(matching: condition, completionHandler: completionHandler)
    }
    @objc open func keys(matching condition: KBGenericCondition, completionHandler: @escaping (Error?, [String]) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler) {
            c in
            self.keys(matching: condition, completionHandler: c)
        }
    }
    
    /**
     Retrieves all the values in the KVS.
     
     - parameter completionHandler: the callback method
     
     */
    open func values(completionHandler: @escaping (Swift.Result<[Any], Error>) -> ()) {
        return self.backingStore.values(completionHandler: completionHandler)
    }
    @objc open func values(completionHandler: @escaping (Error?, [Any]) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler, self.values)
    }
    
    /**
     Retrieves the value corresponding to the key in the KVS.
     
     - parameter key: the key
     - parameter completionHandler: the callback method
     
     */
    open func value(for key: String, completionHandler: @escaping (Swift.Result<Any?, Error>) -> ()) {
        return self.backingStore.value(for: key, completionHandler: completionHandler)
    }
    @objc open func valueForKey(_ key: String, completionHandler: @escaping (Error?, Any?) -> ()) {
        self.value(for: key) { result in
            switch result {
            case .success(let res): completionHandler(nil, res)
            case .failure(let err): completionHandler(err, nil)
            }
        }
    }
    
    /**
     Retrieves the value corresponding to the keys passed as input from the KVS.
     Appends nil for values not present in the KVS for the corresponding key.
     
     - parameter keys: the set of keys
     - parameter completionHandler: the callback method
     
     */
    open func values(for keys: [String], completionHandler: @escaping (Swift.Result<[Any?], Error>) -> ()) {
        return self.backingStore.values(for: keys, completionHandler: completionHandler)
    }
    @objc open func values(for keys: [String], completionHandler: @escaping (Error?, [Any]) -> ()) {
        self.values(for: keys) { result in
            switch result {
            case .success(let v):
                var values = [Any]()
                for nullableValue in v {
                    if nullableValue == nil {
                        values.append(NSNull())
                    } else {
                        values.append(nullableValue!)
                    }
                }
                completionHandler(nil, values)
            case .failure(let err):
                completionHandler(err, [])
            }
        }
    }
    
    /**
     Retrieves the values in the KVS whose keys pass the condition.
     
     - parameter condition: condition the keys need to satisfy
     - parameter completionHandler: the callback method
     
     */
    open func values(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[Any?], Error>) -> ()) {
        return self.backingStore.values(forKeysMatching: condition, completionHandler: completionHandler)
    }
    @objc open func values(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Error?, [Any]) -> ()) {
        self.values(forKeysMatching: condition) { result in
            switch result {
            case .success(let v):
                completionHandler(nil, v.map({ nilToNSNull($0) }))
            case .failure(let err):
                completionHandler(err, [])
            }
        }
    }
}
