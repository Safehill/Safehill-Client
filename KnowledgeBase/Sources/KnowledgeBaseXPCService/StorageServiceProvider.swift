//
//  StorageServiceProvider.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/30/21.
//

import Foundation
import KnowledgeBase


// Distributed notification posted when keys and values are added/updated
let KBPersistentStorageKeysAndValuesUpdatedPrefix = "KBPersistentStorageKeysAndValuesUpdated."
let KBPersistentStorageKeysAndValuesUpdatedPayloadKey = "updated"

// Distributed notification posted when keys are removed
// will be the format KBPersistentStorageKeysRemovedPrefix StoreIdentifier, e.g. "LBPersistentStorageKeysRemoved.Main"
// userInfo will contain KBPersistentStorageKeysRemovedPayloadRemovedKey: [keys]
let KBPersistentStorageKeysRemovedPrefix = "KBPersistentStorageKeysRemoved."
let KBPersistentStorageKeysRemovedPayloadRemovedKey = "removed"


class KBStorageServiceProviderXPC: KBStorageXPCProtocol {
    
    private var cachedHandlers: [String: KBSQLHandler] = [:]
    
    private let serialQueue = DispatchQueue(label: "KBStorageServiceProviderXPC.Serial", qos: .userInteractive, autoreleaseFrequency: .workItem)
    
    func handler(forStoreWithIdentifier identifier: String) throws -> KBSQLHandler {
        log.trace("Looking for store with identifier \(identifier)")
        guard identifier.count > 0 else {
            throw KBError.notSupported
        }
        
        if let handler = cachedHandlers[identifier] {
            return handler
        } else {
            cachedHandlers[identifier] = KBSQLHandler(name: identifier, baseURL: KBKVStore.defaultBaseURL!)
            return cachedHandlers[identifier]!
        }
    }
    
    internal func serializeReadMethodCall<T>(storeIdentifier: String, completionHandler c: @escaping (Error?, T?) -> (), f: @escaping (_: KBSQLHandler) throws -> T?) {
        self.serialQueue.async {
            do {
                let handler = try self.handler(forStoreWithIdentifier: storeIdentifier)
                c(nil, try f(handler))
            } catch {
                c(error, nil)
            }
        }
    }
    
    // SELECT
    
    func keys(inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [String]?) -> ()) {
        serializeReadMethodCall(storeIdentifier: identifier,
                                completionHandler: completionHandler) { handler in
            log.trace("Getting all keys in store with identifier \(identifier)")
            return try handler.keys()
        }
    }
    
    func keys(matching condition: KBGenericCondition, inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [String]?) -> ()) {
        serializeReadMethodCall(storeIdentifier: identifier,
                                completionHandler: completionHandler) { handler in
            log.trace("Getting all keys matching condition \(condition) in store with identifier \(identifier)")
            return try handler.keys(matching: condition)
        }
    }
    
    func value(forKey key: String, inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, Any?) -> ()) {
        serializeReadMethodCall(storeIdentifier: identifier,
                                completionHandler: completionHandler) { handler in
            log.trace("Getting value for key \(key) in store with identifier \(identifier)")
            let value = try handler.values(for: [key]).first
            if let v = value, v is NSNull {
                return nil
            }
            return value
        }
    }
    
    func keysAndValues(inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, KBKVPairs?) -> ()) {
        serializeReadMethodCall(storeIdentifier: identifier,
                                completionHandler: completionHandler) { handler in
            log.trace("Getting all keys and values in store with identifier \(identifier)")
            return try handler.keysAndValues()
        }
    }
    
    func keysAndValues(forKeysMatching condition: KBGenericCondition, inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, KBKVPairs?) -> ()) {
        serializeReadMethodCall(storeIdentifier: identifier,
                                completionHandler: completionHandler) { handler in
            log.trace("Getting all keys and values matching condition \(condition) in store with identifier \(identifier)")
            return try handler.keysAndvalues(forKeysMatching: condition)
        }
    }
    
    func keyValuesAndTimestamps(forKeysMatching condition: KBGenericCondition, inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [KBKVObjcPairWithTimestamp]?) -> ()) {
        serializeReadMethodCall(storeIdentifier: identifier,
                                completionHandler: completionHandler) { handler in
            log.trace("Getting all keys and values matching condition \(condition) in store with identifier \(identifier)")
            return try handler.keyValuesAndTimestamps(forKeysMatching: condition)
        }
    }
    
    func keysAndValues(createdWithin interval: DateInterval, limit: Int, order: ComparisonResult, inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [Date: KBKVPairs]?) -> ()) {
        serializeReadMethodCall(storeIdentifier: identifier,
                                completionHandler: completionHandler) { handler in
            log.trace("Getting all keys and values within interval \(interval) [limit=\(limit)] in store with identifier \(identifier)")
            return try handler.keysAndValues(within: interval, limit: limit, order: order)
        }
    }
    
    func tripleComponents(matching condition: KBTripleCondition?, inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [KBTriple]?) -> ()) {
        serializeReadMethodCall(storeIdentifier: identifier,
                                completionHandler: completionHandler) { handler in
            log.trace("Getting triples matching condition \(condition?.description ?? "<nil>") in store with identifier \(identifier)")
            return try handler.tripleComponents(matching: condition)
        }
    }

    // CREATE/UPDATE
    
    internal func notifyAboutUpsert(inStoreWithIdentifier identifier: String, ofKeys keys: [String]) {
        let notificationName = "\(KBPersistentStorageKeysAndValuesUpdatedPrefix)\(identifier)"
        let userInfo = [KBPersistentStorageKeysAndValuesUpdatedPayloadKey : keys]
        DistributedNotificationCenter.default().post(name: NSNotification.Name(notificationName),
                                                     object: nil,
                                                     userInfo: userInfo)
    }
    
    func save(_ dict: KBKVPairs, toStoreWithIdentifier identifier: String, completionHandler: @escaping KBObjCActionCompletion) {
        self.serialQueue.async {
            log.trace("Getting \(dict.count) keys and values in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                try handler.save(keysAndValues: dict)
                self.notifyAboutUpsert(inStoreWithIdentifier: identifier, ofKeys: Array(dict.keys))
                completionHandler(nil)
            } catch {
                completionHandler(error)
            }
        }
    }
    
    func save(_ dict: KBKVPairs, toSynchedStoreWithIdentifier identifier: String, completionHandler: @escaping KBObjCActionCompletion) {
        log.trace("Getting \(dict.count) keys and values in SYNCHED store with identifier \(identifier)")
        self.save(dict, toStoreWithIdentifier: identifier) { error in
            if error == nil {
                KBCloudKitManager.shared.saveRecords(withDictionary: dict) { _,_,_ in }
            }
            completionHandler(error)
        }
    }

    // DELETE
    
    internal func notifyAboutDeletion(inStoreWithIdentifier identifier: String, ofKeys keys: [String]) {
        let notificationName = "\(KBPersistentStorageKeysRemovedPrefix)\(identifier)"
        let userInfo = [KBPersistentStorageKeysRemovedPayloadRemovedKey : keys]
        DistributedNotificationCenter.default().post(name: NSNotification.Name(notificationName),
                                                     object: nil,
                                                     userInfo: userInfo)
    }
    
    func removeValues(forKeys keys: [String], fromStoreWithIdentifier identifier: String, completionHandler: @escaping KBObjCActionCompletion) {
        self.serialQueue.async {
            log.trace("Removing values for keys \(keys) in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                try handler.removeValues(for: keys)
                self.notifyAboutDeletion(inStoreWithIdentifier: identifier, ofKeys: keys)
                completionHandler(nil)
            } catch {
                completionHandler(error)
            }
        }
    }
    
    func removeValues(forKeys keys: [String], fromSynchedStoreWithIdentifier identifier: String, completionHandler: @escaping KBObjCActionCompletion) {
        self.removeValues(forKeys: keys, fromStoreWithIdentifier: identifier) {
            error in
            if error == nil {
                KBCloudKitManager.shared.removeRecords(forKeys: keys) { _ in }
            }
            completionHandler(error)
        }
    }
    
    func removeValues(forKeysMatching condition: KBGenericCondition, fromStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [String]?) -> ()) {
        self.serialQueue.async {
            log.trace("Removing values for keys matching condition \(condition) in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                let keys = try handler.keys(matching: condition)
                try handler.removeValues(for: keys)
                self.notifyAboutDeletion(inStoreWithIdentifier: identifier, ofKeys: keys)
                completionHandler(nil, keys)
            } catch {
                completionHandler(error, nil)
            }
        }
    }
    
    func removeValues(forKeysMatching condition: KBGenericCondition, fromSynchedStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [String]?) -> ()) {
        self.removeValues(forKeysMatching: condition, fromStoreWithIdentifier: identifier) { error, keys in
            if error == nil {
                KBCloudKitManager.shared.removeRecords(forKeys: keys!) { _ in }
            }
            completionHandler(error, keys)
        }
    }
    
    func removeAll(fromStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [String]?) -> ()) {
        self.serialQueue.async {
            log.trace("Removing all keys and values in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                let keys = try handler.keys()
                try handler.removeValues(for: keys)
                self.notifyAboutDeletion(inStoreWithIdentifier: identifier, ofKeys: keys)
                completionHandler(nil, keys)
            } catch {
                completionHandler(error, nil)
            }
        }
    }
    
    func removeAll(fromSynchedStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, [String]?) -> ()) {
        self.removeAll(fromStoreWithIdentifier: identifier) { error, keys in
            if error == nil {
                KBCloudKitManager.shared.removeRecords(forKeys: keys!) { _ in }
            }
            completionHandler(error, keys)
        }
    }

    // LINKS
    
    func setWeight(forLinkWithLabel predicate: String, between subject: String, and object: String, toValue value: Int, inStoreWithIdentifier identifier: String, completionHandler: @escaping KBObjCActionCompletion) {
        
        self.serialQueue.async {
            log.trace("Setting weight \(value) for triple (\(subject),\(predicate),\(object)) in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                try handler.setWeight(forLinkWithLabel: predicate,
                                      between: subject,
                                      and: object,
                                      toValue: value)
                let keys = KBHexastore.allValues.map {
                    $0.hexaValue(subject: subject, predicate: predicate, object: object)
                }
                self.notifyAboutUpsert(inStoreWithIdentifier: identifier, ofKeys: keys)
                completionHandler(nil)
            } catch {
                completionHandler(error)
            }
        }
    }
    
    func increaseWeight(forLinkWithLabel predicate: String, between subject: String, and object: String, inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, Int) -> ()) {
        
        self.serialQueue.async {
            log.trace("Increasing weight for triple (\(subject),\(predicate),\(object)) in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                let newWeight = try handler.increaseWeight(forLinkWithLabel: predicate,
                                                           between: subject,
                                                           and: object)
                let keys = KBHexastore.allValues.map {
                    $0.hexaValue(subject: subject, predicate: predicate, object: object)
                }
                self.notifyAboutUpsert(inStoreWithIdentifier: identifier, ofKeys: keys)
                completionHandler(nil, newWeight)
            } catch {
                completionHandler(error, kKBInvalidLinkWeight)
            }
        }
    }
    
    func decreaseWeight(forLinkWithLabel predicate: String, between subject: String, and object: String, inStoreWithIdentifier identifier: String, completionHandler: @escaping (Error?, Int) -> ()) {
        self.serialQueue.async {
            log.trace("Decreasing weight for triple (\(subject),\(predicate),\(object)) in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                let newWeight = try handler.decreaseWeight(forLinkWithLabel: predicate,
                                                           between: subject,
                                                           and: object)
                let keys = KBHexastore.allValues.map {
                    $0.hexaValue(subject: subject, predicate: predicate, object: object)
                }
                if newWeight == 0 {
                    self.notifyAboutDeletion(inStoreWithIdentifier: identifier, ofKeys: keys)
                } else {
                    self.notifyAboutUpsert(inStoreWithIdentifier: identifier, ofKeys: keys)
                }
                completionHandler(nil, newWeight)
            } catch {
                completionHandler(error, kKBInvalidLinkWeight)
            }
        }
    }
    
    func dropLink(withLabel predicate: String, between subject: String, and object: String, inStoreWithIdentifier identifier: String, completionHandler: @escaping KBObjCActionCompletion) {
        
        self.serialQueue.async {
            log.trace("Dropping triple (\(subject),\(predicate),\(object)) in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                try handler.dropLink(withLabel: predicate, between: subject, and: object)
                let keys = KBHexastore.allValues.map {
                    $0.hexaValue(subject: subject, predicate: predicate, object: object)
                }
                self.notifyAboutDeletion(inStoreWithIdentifier: identifier, ofKeys: keys)
                completionHandler(nil)
            } catch {
                completionHandler(error)
            }
        }
    }
    
    func dropLinks(withLabel predicate: String?, from subject: String, inStoreWithIdentifier identifier: String, completionHandler: @escaping KBObjCActionCompletion) {
        
        self.serialQueue.async {
            log.trace("Dropping triples matching (\(subject),\(predicate ?? "<nil>"),*) in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                try handler.dropLinks(withLabel: predicate, from: subject)
                let keys = try handler.keys(matching: KBTripleCondition(subject: subject, predicate: predicate, object: nil).rawCondition)
                self.notifyAboutDeletion(inStoreWithIdentifier: identifier, ofKeys: keys)
                completionHandler(nil)
            } catch {
                completionHandler(error)
            }
        }
    }
    
    func dropLinks(between subject: String, and object: String, inStoreWithIdentifier identifier: String, completionHandler: @escaping KBObjCActionCompletion) {
        self.serialQueue.async {
            log.trace("Dropping triples matching (\(subject),*,\(object)) in store with identifier \(identifier)")
            do {
                let handler = try self.handler(forStoreWithIdentifier: identifier)
                try handler.dropLinks(between: subject, and: object)
                let keys = try handler.keys(matching: KBTripleCondition(subject: subject, predicate: nil, object: object).rawCondition)
                self.notifyAboutDeletion(inStoreWithIdentifier: identifier, ofKeys: keys)
                completionHandler(nil)
            } catch {
                completionHandler(error)
            }
        }
    }

    // CLOUD SYNC

    func disableSyncAndDeleteCloudData(completionHandler: @escaping KBObjCActionCompletion) {
        KBCloudKitManager.shared.disableSyncAndDeleteCloudData { result in
            switch result {
            case .success():
                completionHandler(nil)
            case .failure(let error):
                completionHandler(error)
            }
        }
    }
    
}
