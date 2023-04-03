//
//  SPARQLAPIs.swift
//
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import RDFStorage

extension KBKnowledgeStore {
    
    @objc public func importContentsOf(turtleFileAt path: String) async throws {
        return try await withUnsafeThrowingContinuation { continuation in
            let solver = KBSPARQLEndpoint(with: self)
            solver.importTurtle(fromFileAt: path) { result in
                switch result {
                case .success(): continuation.resume()
                case .failure(let error): continuation.resume(throwing: error)
                }
            }
        }
    }
    
    internal func execute(SPARQLQuery query: String, completionHandler: @escaping (Swift.Result<[Any], Error>) -> ()) {
        self.sparqlQueue.async {
            let solver = KBSPARQLEndpoint(with: self)
            
            // @synchronized(solver)
            objc_sync_enter(solver)
            defer { objc_sync_exit(solver) }
            
            log.debug("SPARQL query (\(query))")
            do {
                let results = try solver.execute(query: query)
                completionHandler(.success(results))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
    
    @objc public func importContentsOf(turtleFileAt path: String) async throws {
        return try await withUnsafeThrowingContinuation { continuation in
            let solver = KBSPARQLEndpoint(with: self)
            solver.importTurtle(fromFileAt: path) { result in
                switch result {
                case .success(): continuation.resume()
                case .failure(let error): continuation.resume(throwing: error)
                }
            }
        }
    }
    
    internal func execute(SPARQLQuery query: String, completionHandler: @escaping (Swift.Result<[Any], Error>) -> ()) {
        self.sparqlQueue.async {
            let solver = KBSPARQLEndpoint(with: self)
            
            // @synchronized(solver)
            objc_sync_enter(solver)
            defer { objc_sync_exit(solver) }
            
            log.debug("SPARQL query (\(query))")
            do {
                let results = try solver.execute(query: query)
                completionHandler(.success(results))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
    
    /**
     Executes the SPARQL SELECT query and returns all the bounded values in the projection.
     
     - parameter query: the SPARQL SELECT query to execute
     */
    @objc public func execute(SPARQLQuery query: String) async throws -> [Any] {
        return try await KBModernAsyncMethodReturningInitiable { c in
            self.execute(SPARQLQuery: query, completionHandler: c)
        }
//        async let result = { () -> [Any] in
//            let solver = KBSPARQLEndpoint(with: self)
//            return try solver.execute(query: query)
//        }
//        return try await result()
    }
}


// MARK: - TripleStore protocol
extension KBKnowledgeStore : TripleStore {
    public func insertTriple(withSubject subject: String, predicate: String, object: String) async throws {
        let subject = self.entity(withIdentifier: subject)
        let object = self.entity(withIdentifier: object)
        
        try await subject.link(to: object, withPredicate: predicate)
    }
}
