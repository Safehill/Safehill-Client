//
//  TripleStoreWriteAPIs.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

//import RDFStorage

extension KBKnowledgeStore {
    
    /**
     Remove the entity with a certain identifier.
     WARN: This will remove all the connections to this entity in the graph.
     
     - parameter identifier: the identifier
     */
    public func removeEntity(_ identifier: Label, completionHandler: @escaping KBActionCompletion) {
        log.trace("[<\(identifier)> $? $?] or [<\(identifier)> $? $?]")
        
        let subjectMatches = KBTripleCondition(
            subject: identifier,
            predicate: nil,
            object: nil
        )
        let objectMatches = KBTripleCondition(
            subject: nil,
            predicate: nil,
            object: identifier
        )
        let condition = subjectMatches.or(objectMatches)
        
        self.backingStore.removeValues(forKeysMatching: condition.rawCondition) { [weak self] result in
            self?.delegate?.linkedDataDidChange()
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(_):
                completionHandler(.success(()))
                self?.delegate?.linkedDataDidChange()
            }
        }
    }
    
    public func verify(path: KBPath, completionHandler: @escaping (Swift.Result<Bool, Error>) -> ()) {
        self.backingStore.verify(path: path, completionHandler: completionHandler)
    }
}
