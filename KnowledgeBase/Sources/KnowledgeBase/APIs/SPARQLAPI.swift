//
//  SPARQLAPIs.swift
//
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import RDFStorage

extension KBKnowledgeStore {
    
    public func importContentsOf(turtleFileAt path: String, completionHandler: @escaping KBActionCompletion) {
        let solver = KBSPARQLEndpoint(with: self)
        solver.importTurtle(fromFileAt: path, completionHandler: completionHandler)
    }
    
    /**
     Executes the SPARQL SELECT query and returns all the bounded values in the projection.
     
     - parameter query: the SPARQL SELECT query to execute
     - parameter completionHandler: the callback method
     */
    public func execute(SPARQLQuery query: String, completionHandler: @escaping (Swift.Result<[Any], Error>) -> ()) {
        self.sparqlQueue.async {
            let solver = KBSPARQLEndpoint(with: self)
            
            // @synchronized(solver)
            objc_sync_enter(solver)
            defer { objc_sync_exit(solver) }
            
            log.debug("SPARQL query (\(query, privacy: .public)")
            do {
                let results = try solver.execute(query: query)
                completionHandler(.success(results))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
}


// MARK: - TripleStore protocol
extension KBKnowledgeStore : TripleStore {
    public func insertTriple(withSubject subject: String, predicate: String, object: String, completionHandler: @escaping (Error?) -> ()) {
        let subject = self.entity(withIdentifier: subject)
        let object = self.entity(withIdentifier: object)
        
        subject.link(to: object, withPredicate: predicate) { result in
            switch result {
            case .failure(let err): completionHandler(err)
            case .success: completionHandler(nil)
            }
        }
    }
}
