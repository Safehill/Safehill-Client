//
//  SynchedSQLXPC.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/28/21.
//

import Foundation

class KBCloudKitSQLBackingStore : KBSQLBackingStore {
    
    override var name: String {
        get { return KnowledgeBaseSQLSynchedIdentifier }
        set(v) {}
    }
    
    class override func mainInstance() -> Self {
        return self.init(name: KnowledgeBaseSQLSynchedIdentifier)
    }
    
    // MARK: SELECT
    
    func triplesComponents(matching condition: KBTripleCondition?,
                           completionHandler: @escaping (Swift.Result<[KBTriple], Error>) -> ()) {
        log.fault("\(KnowledgeBaseBundleIdentifier, privacy: .public) store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func verify(path: KBPath, completionHandler: @escaping (Swift.Result<Bool, Error>) -> ()) {
        log.fault("\(KnowledgeBaseBundleIdentifier, privacy: .public) store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    //MARK: INSERT
    
    override func writeBatch() -> KBKVStoreWriteBatch {
        return KBCloudKitSQLWriteBatch(backingStore: self)
    }
    
    func setWeight(forLinkWithLabel predicate: String,
                   between subjectIdentifier: String,
                   and objectIdentifier: String,
                   toValue newValue: Int,
                   completionHandler: @escaping KBActionCompletion) {
        log.fault("\(KnowledgeBaseBundleIdentifier, privacy: .public) store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func increaseWeight(forLinkWithLabel predicate: Label,
                        between subjectIdentifier: Label,
                        and objectIdentifier: Label,
                        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()) {
        log.fault("\(KnowledgeBaseBundleIdentifier, privacy: .public) store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func decreaseWeight(forLinkWithLabel predicate: Label,
                        between subjectIdentifier: Label,
                        and objectIdentifier: Label,
                        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()) {
        log.fault("\(KnowledgeBaseBundleIdentifier, privacy: .public) store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    //MARK: DELETE

    func dropLink(withLabel predicate: Label,
                  between subjectIdentifier: Label,
                  and objectIdentifier: Label,
                  completionHandler: @escaping KBActionCompletion) {
        log.fault("\(KnowledgeBaseBundleIdentifier, privacy: .public) store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func dropLinks(withLabel predicate: Label?,
                   from subjectIdentifier: Label,
                   completionHandler: @escaping KBActionCompletion) {
        log.fault("\(KnowledgeBaseBundleIdentifier, privacy: .public) store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func dropLinks(between subjectIdentifier: Label,
                   and objectIdentifier: Label,
                   completionHandler: @escaping KBActionCompletion) {
        log.fault("\(KnowledgeBaseBundleIdentifier, privacy: .public) store is not meant to store graphs")
        completionHandler(.failure(KBError.notSupported))
    }
    
    func disableSyncAndDeleteCloudData(completionHandler: @escaping KBActionCompletion) {
        // TODO: Implement
    }
}
