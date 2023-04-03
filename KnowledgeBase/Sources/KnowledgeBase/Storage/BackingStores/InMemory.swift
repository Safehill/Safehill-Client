//
//  InMemory.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation


class KBInMemoryBackingStore : KBSQLBackingStoreProtocol {
    
    var name: String {
        get { return KnowledgeBaseInMemoryIdentifier }
        set(v) {}
    }
    
    internal let sqlHandler: KBSQLHandler // In-memory SQL database
    
    init() {
        self.sqlHandler = KBSQLHandler.inMemoryHandler()!
    }
    
    func writeBatch() -> KBKVStoreWriteBatch {
        return KBSQLWriteBatch(backingStore: self)
    }
}
