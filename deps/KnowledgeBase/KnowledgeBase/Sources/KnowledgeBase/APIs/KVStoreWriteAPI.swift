//
//  KVStoreWriteAPIs.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import Foundation

func KBObjectiveCAPIResultReturningVoid(completionHandler: @escaping (Error?) -> (), _ f: @escaping (@escaping (Swift.Result<Void, Error>) -> ()) -> ()) {
    f { result in
        switch result {
        case .success(): completionHandler(nil)
        case .failure(let err): completionHandler(err)
        }
    }
}


extension KBKVStore {
    
    /**
     Assign a value, or nil, to a specific key in the KVS.
     
     - parameter value: the value
     - parameter key: the key
     - parameter completionHandler: the callback method
     
     */
    public func set(value: Any?, for key: String, completionHandler: @escaping KBActionCompletion) {
        guard self.supportsSecureCoding(value) else {
            log.error("Won't save a non NSSecureCoding compliant value (\(String(describing: value)) for key (\(key))")
            completionHandler(.failure(KBError.unexpectedData(value)))
            return
        }
        
        let writeBatch = self.writeBatch()
        writeBatch.set(value: value, for: key)
        writeBatch.write { result in
            completionHandler(result)
            self.delegate?.kvDataDidChange(addedKeys: [key], removedKeys: [])
        }
    }
    @objc public func set(value: Any?, for key: String, completionHandler: @escaping KBObjCActionCompletion) {
        KBObjectiveCAPIResultReturningVoid(completionHandler: completionHandler) { c in
            self.set(value: value, for: key, completionHandler: c)
        }
    }
    
    /**
     Remove a tuple in the KVS, given its key.
     
     - parameter key: the key
     - parameter completionHandler: the callback method
     
     */
    public func removeValue(for key: String, completionHandler: @escaping KBActionCompletion) {
        self.backingStore.removeValue(for: key) { result in
            completionHandler(result)
            self.delegate?.kvDataDidChange(addedKeys: [], removedKeys: [key])
        }
    }
    @objc public func removeValue(for key: String, completionHandler: @escaping KBObjCActionCompletion) {
        KBObjectiveCAPIResultReturningVoid(completionHandler: completionHandler) { c in
            self.removeValue(for: key, completionHandler: c)
        }
    }
    
    /**
     Remove a set of tuples in the KVS, given their keys.
     Blocking version
     
     - parameter keys: the keys
     - parameter completionHandler: the callback method
     
     */
    public func removeValues(for keys: [String], completionHandler: @escaping KBActionCompletion) {
        self.backingStore.removeValues(for: keys, completionHandler: completionHandler)
    }
    @objc public func removeValues(for keys: [String], completionHandler: @escaping KBObjCActionCompletion) {
        KBObjectiveCAPIResultReturningVoid(completionHandler: completionHandler) { c in
            self.removeValues(for: keys, completionHandler: c)
        }
    }
    
    /**
     Remove a set of tuples in the KVS, matching the condition.
     
     - parameter condition: the condition
     - parameter completionHandler: the callback method. Returns the keys removed
     */
    public func removeValues(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        self.backingStore.removeValues(forKeysMatching: condition, completionHandler: completionHandler)
    }
    @objc func removeValues(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Error?, [String]) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler) { c in
            self.removeValues(forKeysMatching: condition, completionHandler: c)
        }
    }
    
    /**
     Remove all values in the KVS
     
     - parameter completionHandler: the callback method. Returns the keys removed
     
     */
    public func removeAll(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        self.backingStore.removeAll { result in
            completionHandler(result)
            self.delegate?.kvWasDestroyed()
        }
    }
    @objc func removeAll(completionHandler: @escaping (Error?, [String]) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler, self.removeAll(completionHandler:))
    }
    
    /**
     Disable CloudKit syncing and remove all the data in the cloud
     */
    public func disableSyncAndDeleteCloudData(completionHandler: @escaping KBActionCompletion) {
        self.backingStore.disableSyncAndDeleteCloudData(completionHandler: completionHandler)
    }
    @objc public func disableSyncAndDeleteCloudData(completionHandler: @escaping KBObjCActionCompletion) {
        KBObjectiveCAPIResultReturningVoid(completionHandler: completionHandler, self.disableSyncAndDeleteCloudData(completionHandler:))
    }
    
}
