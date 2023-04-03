//
//  SPARQLEndpoint.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/8/21.
//

import Foundation
import RDFStorage

class KBSPARQLEndpoint {
    internal let rdfStore: BaseRDFStore
    
    public init(with knowledgeStore: KBKnowledgeStore) {
        self.rdfStore = BaseRDFStore(tripleStore: knowledgeStore)
    }
    
    public func execute(query: String) throws -> [Any] {
        try self.rdfStore.execute(SPARQLQuery: query)
    }
    
    public func importTurtle(fromFileAt path: String, completionHandler: @escaping KBActionCompletion) {
        self.rdfStore.importTriples(fromFileAtPath: path, completionHandler: { error in
            if let e = error {
                return completionHandler(.failure(e))
            }
            return completionHandler(.success(()))
        })
    }
    
}
