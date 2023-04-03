//
//  TripleStoreReadAPIs.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import Foundation

extension KBKnowledgeStore {
    /**
     Returns all entities in the graph.
     
     - parameter completionHandler: the callback method
     
     */
    @objc public func entities() async throws -> [KBEntity] {
        var uniqueIDs = Set<Label>()
        
        let results = try await self.backingStore.values()
            
        for value in results {
            if let triple = value as? KBTriple {
                uniqueIDs.insert(triple.subject)
                uniqueIDs.insert(triple.object)
            }
        }
            
        return uniqueIDs.map {
            self.entity(withIdentifier: $0)
        }
    }
    
    /**
     Matches triples need against the condition passed as argument
     
     - parameter condition: matches only triples having satisfying this condition.
     If nil, matches all triples
     
     - returns: The array of triples in a dictionary with keys: subject, predicate, object
     */
    @objc public func triples(matching condition: KBTripleCondition?) async throws -> [KBTriple] {
        return try await self.backingStore.triplesComponents(matching: condition)
    }
}
