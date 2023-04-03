//
//  UserDefaults.swift
//
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation


internal func KBAsyncMethodReturningVoid
(_ c: @escaping (Swift.Result<Void, Error>) -> (), f: @escaping () -> ()) {
    return c(.success(f()))
}

internal func KBAsyncMethodReturningInitiable<T: Initiable>(_ c: @escaping (Swift.Result<T, Error>) -> (), f: @escaping () -> T) {
    return c(.success(f()))
}


class KBUserDefaultsBackingStore : KBBackingStore {
    
    private var kv: UserDefaults
    var name: String {
        get { return KnowledgeBaseUserDefaultsIdentifier }
        set(v) {}
    }

    init() {
        log.trace("initializing .UserDefaults Store with suite name \(KnowledgeBaseBundleIdentifier, privacy: .public)")

        if let defaults = UserDefaults(suiteName: KnowledgeBaseBundleIdentifier) {
            self.kv = defaults
        } else {
            log.fault("Can't initialize defaults with suiteName \(KnowledgeBaseBundleIdentifier, privacy: .public). Using standard user defaults")
            self.kv = UserDefaults()
        }
    }

    internal func synchronize() {
        self.kv.synchronize()
    }
    
    func writeBatch() -> KBKVStoreWriteBatch {
        return KBUserDefaultsWriteBatch(backingStore: self)
    }

    //MARK: SELECT

    func keys(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        KBAsyncMethodReturningInitiable(completionHandler) {
            return ((self.kv.dictionaryRepresentation() as NSDictionary).allKeys as! [String])
        }
    }
    
    func keys(matching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        KBAsyncMethodReturningInitiable(completionHandler) {
            let filteredKeys = ((self.kv.dictionaryRepresentation() as NSDictionary).allKeys.filter {
                k -> Bool in
                return condition.evaluate(on: k)
            })
            return filteredKeys as! [String]
        }
    }
    
    internal func value(for key: String) -> Any? {
        return self.kv.object(forKey: key)
    }
    
    func value(for key: String, completionHandler: @escaping (Swift.Result<Any?, Error>) -> ()) {
        KBAsyncMethodReturningInitiable(completionHandler) {
            return self.value(for: key)
        }
    }

    func values(completionHandler: @escaping (Swift.Result<[Any], Error>) -> ()) {
        KBAsyncMethodReturningInitiable(completionHandler) {
            return (self.kv.dictionaryRepresentation() as NSDictionary).allValues
        }
    }
    
    func values(for keys: [String], completionHandler: @escaping (Swift.Result<[Any?], Error>) -> ()) {
        KBAsyncMethodReturningInitiable(completionHandler) {
            return keys.map { key in self.value(for: key) }
        }
    }
    
    func values(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[Any?], Error>) -> ()) {
        self.keys() { result in
            switch result {
            case .success(let allKeys):
                let filteredKeys = allKeys.filter { condition.evaluate(on: $0) }
                let values = filteredKeys.map { key in self.value(for: key) }
                completionHandler(.success(values))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func dictionaryRepresentation(completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ()) {
        KBAsyncMethodReturningInitiable(completionHandler) {
            return self.kv.dictionaryRepresentation()
        }
    }
    
    func dictionaryRepresentation(forKeysMatching condition: KBGenericCondition,
                                  completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ()) {
        self.keys() { result in
            switch result {
            case .success(let allKeys):
                let dictionary = allKeys.reduce([:], {
                    (dict, k) in
                    if condition.evaluate(on: k) {
                        var newDict = dict
                        newDict[k] = self.value(for: k)!
                        return newDict
                    }
                    return dict
                })
                completionHandler(.success(dictionary as! KBKVPairs))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func dictionaryRepresentation(createdWithin interval: DateInterval,
                                  limit: Int?,
                                  order: ComparisonResult,
                                  completionHandler: @escaping (Swift.Result<[Date: KBKVPairs], Error>) -> ()) {
        log.fault(".UserDefaults store does not support timestamps")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func triplesComponents(matching condition: KBTripleCondition?,
                           completionHandler: @escaping (Swift.Result<[KBTriple], Error>) -> ()) {
        log.fault(".UserDefaults store is not meant to store triples")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func set(value: Any?, for key: String, completionHandler: @escaping KBActionCompletion) {
        KBAsyncMethodReturningVoid(completionHandler) {
            self.kv.set(value, forKey: key)
        }
    }
        
    func removeValue(for key: String, completionHandler: @escaping KBActionCompletion) {
        KBAsyncMethodReturningVoid(completionHandler) {
            self.kv.removeObject(forKey: key)
            self.synchronize()
        }
    }
    
    func removeValues(for keys: [String], completionHandler: @escaping KBActionCompletion) {
        KBAsyncMethodReturningVoid(completionHandler) {
            for key in keys {
                self.kv.removeObject(forKey: key)
            }
            self.synchronize()
        }
    }
    
    func removeValues(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        self.keys() { result in
            switch result {
            case .success(let allKeys):
                var removedKeys = [String]()
                for key in allKeys {
                    if condition.evaluate(on: key) {
                        self.kv.removeObject(forKey: key)
                        removedKeys.append(key)
                    }
                }
                self.synchronize()
                completionHandler(.success(removedKeys))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func removeAll(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        self.keys() { result in
            switch result {
            case .success(let allKeys):
                var removedKeys = [String]()
                for key in allKeys {
                    self.kv.removeObject(forKey: key)
                    removedKeys.append(key)
                }
                self.synchronize()
                completionHandler(.success(removedKeys))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    // GRAPH LINKS
    func verify(path: KBPath, completionHandler: @escaping (Swift.Result<Bool, Error>) -> ()) {
        log.fault(".UserDefaults store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func setWeight(forLinkWithLabel predicate: String,
                   between subjectIdentifier: String,
                   and objectIdentifier: String,
                   toValue newValue: Int,
                   completionHandler: @escaping KBActionCompletion) {
        log.fault(".UserDefaults store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func increaseWeight(forLinkWithLabel predicate: String,
                        between subjectIdentifier: String,
                        and objectIdentifier: String,
                        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()) {
        log.fault(".UserDefaults store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func decreaseWeight(forLinkWithLabel predicate: Label,
                        between subjectIdentifier: Label,
                        and objectIdentifier: Label,
                        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()) {
        log.fault(".UserDefaults store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func dropLink(withLabel predicate: String,
                  between subjectIdentifier: String,
                  and objectIdentifier: String,
                  completionHandler: @escaping KBActionCompletion) {
        log.fault(".UserDefaults store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func dropLinks(withLabel predicate: String?,
                   from subjectIdentifier: String,
                   completionHandler: @escaping KBActionCompletion) {
        log.fault(".UserDefaults store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func dropLinks(between subjectIdentifier: String,
                   and objectIdentifier: String,
                   completionHandler: @escaping KBActionCompletion) {
        log.fault(".UserDefaults store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    // CLOUD SYNC
    func disableSyncAndDeleteCloudData(completionHandler: @escaping KBActionCompletion) {
        log.fault(".UserDefaults store does not support cloud syncing")
        completionHandler(.failure(KBError.notSupported))
    }
    
}

