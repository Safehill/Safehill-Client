//
//  BackingStore.swift
//
//
//  Created by Gennaro Frazzingaro on 6/19/21.
//

import Foundation

typealias KBBackingStoreProtocol = KBAsynchronousBackingStore & KBSynchronousBackingStore


protocol KBAsynchronousBackingStore {
    
    // MARK: KVStore
    func keys(completionHandler: @escaping (Swift.Result<[String], Error>) -> ())
    func keys(matching: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ())
    func value(for key: String, completionHandler: @escaping (Swift.Result<Any?, Error>) -> ())
    func values(completionHandler: @escaping (Swift.Result<[Any], Error>) -> ())
    func values(for keys: [String], completionHandler: @escaping (Swift.Result<[Any?], Error>) -> ())
    func values(forKeysMatching: KBGenericCondition, completionHandler: @escaping (Swift.Result<[Any?], Error>) -> ())
    func keyValuesAndTimestamps(forKeysMatching: KBGenericCondition, completionHandler: @escaping (Swift.Result<[KBKVPairWithTimestamp], Error>) -> ())
    func dictionaryRepresentation(completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ())
    func dictionaryRepresentation(forKeysMatching: KBGenericCondition,
                                  completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ())
    func dictionaryRepresentation(createdWithin interval: DateInterval,
                                  limit: Int?,
                                  order: ComparisonResult,
                                  completionHandler: @escaping (Swift.Result<[Date: KBKVPairs], Error>) -> ()) 
    func set(value: Any?, for key: String, completionHandler: @escaping KBActionCompletion)
    func removeValue(for key: String, completionHandler: @escaping KBActionCompletion)
    func removeValues(for keys: [String], completionHandler: @escaping KBActionCompletion)
    func removeValues(forKeysMatching condition: KBGenericCondition, completionHandler: @escaping (Swift.Result<[String], Error>) -> ())
    func removeAll(completionHandler: @escaping (Swift.Result<[String], Error>) -> ())
    
    // MARK: KnowledgeStore
    func triplesComponents(matching condition: KBTripleCondition?,
                           completionHandler: @escaping (Swift.Result<[KBTriple], Error>) -> ())
    func verify(path: KBPath, completionHandler: @escaping (Swift.Result<Bool, Error>) -> ())
    func setWeight(forLinkWithLabel predicate: String,
                   between subjectIdentifier: String,
                   and objectIdentifier: String,
                   toValue newValue: Int,
                   completionHandler: @escaping KBActionCompletion)
    func increaseWeight(forLinkWithLabel predicate: String,
                        between subjectIdentifier: String,
                        and objectIdentifier: String,
                        completionHandler: @escaping (Swift.Result<Int, Error>) -> ())
    func decreaseWeight(forLinkWithLabel predicate: Label,
                        between subjectIdentifier: Label,
                        and objectIdentifier: Label,
                        completionHandler: @escaping (Swift.Result<Int, Error>) -> ())
    func dropLink(withLabel predicate: String,
                  between subjectIdentifier: String,
                  and objectIdentifier: String,
                  completionHandler: @escaping KBActionCompletion)
    func dropLinks(withLabel predicate: String?,
                   from subjectIdentifier: String,
                   completionHandler: @escaping KBActionCompletion)
    func dropLinks(between subjectIdentifier: String,
                   and objectIdentifier: String,
                   completionHandler: @escaping KBActionCompletion)
    
    // MARK: Cloud sync
    func disableSyncAndDeleteCloudData(completionHandler: @escaping KBActionCompletion)
}

/**
  Blocking API for the KBAsynchronousBackingStore.
  Uses semaphores to synchronize the threads.
 */
protocol KBSynchronousBackingStore : KBAsynchronousBackingStore {
    
    // MARK: KVStore
    func keys() throws -> [String]
    func keys(matching: KBGenericCondition) throws -> [String]
    func value(for key: String) throws -> Any?
    func values() throws -> [Any]
    func values(for keys: [String]) throws -> [Any?]
    func values(forKeysMatching: KBGenericCondition) throws -> [Any?]
    func keyValuesAndTimestamps(forKeysMatching: KBGenericCondition) throws -> [KBKVPairWithTimestamp]
    func dictionaryRepresentation() throws -> KBKVPairs
    func dictionaryRepresentation(forKeysMatching: KBGenericCondition) throws -> KBKVPairs
    func dictionaryRepresentation(createdWithin: DateInterval, limit: Int?, order: ComparisonResult) throws -> [Date: KBKVPairs]
    func set(value: Any?, for key: String) throws
    func removeValue(for key: String) throws
    func removeValues(for keys: [String]) throws
    func removeValues(forKeysMatching condition: KBGenericCondition) throws -> [String]
    func removeAll() throws -> [String]
    
    // MARK: KnowledgeStore
    func triplesComponents(matching condition: KBTripleCondition?) throws -> [KBTriple]
    func verify(path: KBPath) throws -> Bool
    func setWeight(forLinkWithLabel predicate: String,
                   between subjectIdentifier: String,
                   and objectIdentifier: String,
                   toValue newValue: Int) throws
    func increaseWeight(forLinkWithLabel predicate: String,
                        between subjectIdentifier: String,
                        and objectIdentifier: String) throws -> Int
    func decreaseWeight(forLinkWithLabel predicate: Label,
                        between subjectIdentifier: Label,
                        and objectIdentifier: Label) throws -> Int
    func dropLink(withLabel predicate: String,
                  between subjectIdentifier: String,
                  and objectIdentifier: String) throws
    func dropLinks(withLabel predicate: String?,
                   from subjectIdentifier: String) throws
    func dropLinks(between subjectIdentifier: String,
                   and objectIdentifier: String) throws
    
    // MARK: Cloud sync
    func disableSyncAndDeleteCloudData() throws
}


/**
 * This protocol defaults the synchronous methods to call the asynchronous one.
 */
extension KBSynchronousBackingStore {
    
    func keys() throws -> [String] {
        return try KBSyncMethodReturningInitiable(execute: self.keys)
    }
    
    func keys(matching condition: KBGenericCondition) throws -> [String] {
        return try KBSyncMethodReturningInitiable {
            (completionHandler) in
            self.keys(matching: condition, completionHandler: completionHandler)
        }
    }
    
    func value(for key: String) throws -> Any? {
        return try KBSyncMethodReturningInitiable { c in
            self.value(for: key, completionHandler: c)
        }
    }
    
    func set(value: Any?, for key: String) throws {
        return try KBSyncMethodReturningVoid { c in
            self.set(value: value, for: key, completionHandler: c)
        }
    }
    
    func values() throws -> [Any] {
        return try KBSyncMethodReturningInitiable(execute: self.values)
    }
    
    func values(for keys: [String]) throws -> [Any?] {
        return try KBSyncMethodReturningInitiable { c in
            self.values(for: keys, completionHandler: c)
        }
    }
    
    func values(forKeysMatching condition: KBGenericCondition) throws -> [Any?] {
        return try KBSyncMethodReturningInitiable {
            (completionHandler) in
            self.values(forKeysMatching: condition, completionHandler: completionHandler)
        }
    }
    
    func keyValuesAndTimestamps(forKeysMatching condition: KBGenericCondition) throws -> [KBKVPairWithTimestamp] {
        return try KBSyncMethodReturningInitiable {
            (completionHandler) in
            self.keyValuesAndTimestamps(forKeysMatching: condition, completionHandler: completionHandler)
        }
    }
    
    func dictionaryRepresentation() throws -> KBKVPairs {
        return try KBSyncMethodReturningInitiable(execute: self.dictionaryRepresentation)
    }
    
    func dictionaryRepresentation(forKeysMatching condition: KBGenericCondition) throws -> KBKVPairs {
        return try KBSyncMethodReturningInitiable {
            (completionHandler) in
            self.dictionaryRepresentation(forKeysMatching: condition, completionHandler: completionHandler)
        }
    }
    
    func dictionaryRepresentation(createdWithin interval: DateInterval, limit: Int?, order: ComparisonResult) throws -> [Date: KBKVPairs] {
        return try KBSyncMethodReturningInitiable {
            (completionHandler) in
            self.dictionaryRepresentation(createdWithin: interval, limit: limit, order: order, completionHandler: completionHandler)
        }
    }
    
    func removeValue(for key: String) throws {
        try KBSyncMethodReturningVoid { c in
            self.removeValue(for: key, completionHandler: c)
        }
    }
    
    func removeValues(for keys: [String]) throws {
        try KBSyncMethodReturningVoid { c in
            self.removeValues(for: keys, completionHandler: c)
        }
    }
    
    func removeValues(forKeysMatching condition: KBGenericCondition) throws -> [String] {
        try KBSyncMethodReturningInitiable { c in
            self.removeValues(forKeysMatching: condition, completionHandler: c)
        }
    }
    
    func removeAll() throws -> [String] {
        try KBSyncMethodReturningInitiable(execute: self.removeAll)
    }
    
    func triplesComponents(matching condition: KBTripleCondition?) throws -> [KBTriple] {
        return try KBSyncMethodReturningInitiable { c in
            self.triplesComponents(matching: condition, completionHandler: c)
        }
    }
    
    func verify(path: KBPath) throws -> Bool {
        try KBSyncMethodReturningInitiable { c in
            self.verify(path: path, completionHandler: c)
        }
    }
    
    func setWeight(forLinkWithLabel predicate: String, between subjectIdentifier: String, and objectIdentifier: String, toValue newValue: Int) throws {
        try KBSyncMethodReturningVoid { c in
            self.setWeight(forLinkWithLabel: predicate,
                           between: subjectIdentifier,
                           and: objectIdentifier,
                           toValue: newValue,
                           completionHandler: c)
        }
    }
    
    func increaseWeight(forLinkWithLabel predicate: String, between subjectIdentifier: String, and objectIdentifier: String) throws -> Int {
        try KBSyncMethodReturningInitiable { c in
            self.increaseWeight(forLinkWithLabel: predicate,
                                between: subjectIdentifier,
                                and: objectIdentifier,
                                completionHandler: c)
        }
    }
    
    func decreaseWeight(forLinkWithLabel predicate: Label,
                        between subjectIdentifier: Label,
                        and objectIdentifier: Label) throws -> Int {
        try KBSyncMethodReturningInitiable { c in
            self.decreaseWeight(forLinkWithLabel: predicate,
                                between: subjectIdentifier,
                                and: objectIdentifier,
                                completionHandler: c)
        }
    }
    
    func dropLink(withLabel predicate: String,
                  between subjectIdentifier: String,
                  and objectIdentifier: String) throws {
        try KBSyncMethodReturningVoid { c in
            self.dropLink(withLabel: predicate,
                          between: subjectIdentifier,
                          and: objectIdentifier,
                          completionHandler: c)
        }
    }
    
    func dropLinks(withLabel predicate: String?,
                   from subjectIdentifier: String) throws {
        try KBSyncMethodReturningVoid { c in
            self.dropLinks(withLabel: predicate,
                           from: subjectIdentifier,
                           completionHandler: c)
        }
    }
    
    func dropLinks(between subjectIdentifier: String,
                   and objectIdentifier: String) throws {
        try KBSyncMethodReturningVoid { c in
            self.dropLinks(between: subjectIdentifier,
                           and: objectIdentifier,
                           completionHandler: c)
        }
    }
    
    func disableSyncAndDeleteCloudData() throws {
        try KBSyncMethodReturningVoid { c in
            self.disableSyncAndDeleteCloudData(completionHandler: c)
        }
    }
}

/**
 Protocol that every backend needs to implement
 */
protocol KBBackingStore: KBBackingStoreProtocol {
    var name: String { get }
    func writeBatch() -> KBKVStoreWriteBatch
}

/**
 BackingStore using a SQL persistent storage handler.
 */
protocol KBPersistentBackingStore: KBBackingStore {
    var sqlHandler: KBSQLHandler { get }
}


