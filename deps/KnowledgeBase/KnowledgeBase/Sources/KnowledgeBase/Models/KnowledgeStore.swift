//
//  KnowledgeStore.swift
//
//
//  Created by Gennaro Frazzingaro on 7/16/21.
//

import Foundation

@objc(KBKnowledgeStore)
open class KBKnowledgeStore : KBKVStore {
    internal let sparqlQueue: DispatchQueue
    
    public static let inMemoryGraph = KBKnowledgeStore.inMemoryStore()
    
    override init(_ location: Location) {
        self.sparqlQueue = DispatchQueue(label: "SPARQL",
                                         qos: .userInteractive)
        super.init(location)
    }
    
    @objc open override class func defaultStore() -> KBKnowledgeStore {
        return KBKnowledgeStore.store(withName: "")
    }
    
    @objc open override class func defaultSynchedStore() -> KBKnowledgeStore {
        return KBKnowledgeStore.synchedStore(withName: "")
    }

    @objc open override class func inMemoryStore() -> KBKnowledgeStore {
        return KBKnowledgeStore.store(Location.inMemory)
    }

    @objc open override class func userDefaultsStore() -> KBKnowledgeStore {
        return KBKnowledgeStore.store(Location.userDefaults)
    }
    
    open override class func store(withName name: String) -> KBKnowledgeStore {
        if name == KnowledgeBaseInMemoryIdentifier {
            return KBKnowledgeStore.store(.inMemory)
        } else if name == KnowledgeBaseUserDefaultsIdentifier {
            return KBKnowledgeStore.store(.userDefaults)
        }
        return KBKnowledgeStore.store(Location.sql(name))
    }
    
    open override class func store(_ location: Location) -> KBKnowledgeStore {
        return KBKnowledgeStore(location)
    }
    
    @objc open override class func synchedStore(withName name: String) -> KBKnowledgeStore {
        return KBKnowledgeStore(Location.sqlSynched(name))
    }
}


// MARK: - KnowledgeStore Read API

public extension KBKnowledgeStore {
    
    //MARK: entity(withIdentifier:)
    
    /**
     Construct a KBEntity object
     
     - parameter identifier: the unique identifier of the entity
     - returns: A KBEntity object
     
     */
    @objc func entity(withIdentifier identifier: Label) -> KBEntity {
        return KBEntity(identifier: identifier, knowledgeStore: self)
    }
}

