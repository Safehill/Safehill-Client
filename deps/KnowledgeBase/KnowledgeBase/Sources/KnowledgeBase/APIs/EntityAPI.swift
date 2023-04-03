//
//  EntityAPI.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import Foundation

extension KBEntity {
    
    // MARK: KBEntity attributes
    
    public func value(forAttribute key: String,
                      completionHandler: @escaping (Swift.Result<Any?, Error>) -> ()) {
        let key = KBHexastore.JOINER.combine(self.identifier, key)
        self.store.value(for: key, completionHandler: completionHandler)
    }
    
    public func set(value: Any,
                    forAttribute key: String,
                    completionHandler: @escaping KBActionCompletion) {
        let writeBatch = self.store.backingStore.writeBatch()
        let entityKey = KBHexastore.JOINER.combine(self.identifier, key)
        writeBatch.set(value: value, for: entityKey)
        writeBatch.write(completionHandler: completionHandler)
    }
    
    public func setAttributes(_ keysAndValues: [String: Any], completionHandler: @escaping KBActionCompletion) {
        let writeBatch = self.store.backingStore.writeBatch()
        for (key, value) in keysAndValues {
            let entityKey = KBHexastore.JOINER.combine(self.identifier, key)
            writeBatch.set(value: value, for: entityKey)
        }
        
        writeBatch.write(completionHandler: completionHandler)
    }

    public func removeAttribute(named key: String, completionHandler: @escaping KBActionCompletion) {
        let entityKey = KBHexastore.JOINER.combine(self.identifier, key)
        self.store.removeValue(for: entityKey, completionHandler: completionHandler)
    }


    // MARK: Linking/Unlinking

    /**
     Create a labeled connection between this KBEntity and the one passed as parameter

     If the target object has any watcher attached then these will all fire

     - parameter target: the KBEntity to connect to
     - parameter predicate: the label on the link
     - parameter completionHandler: the callback method
     
     */
    public func link(to target: KBEntity,
                     withPredicate predicate: Label,
                     completionHandler: @escaping KBActionCompletion) {
        log.trace("Linking [<\(self)> <\(predicate)> <\(target)>]")

        let subject = self.identifier
        let predicate = predicate
        let object = target.identifier
        
        let dispatch = KBTimedDispatch()
        
        // Increment the weight of the link and update the hexastore
        self.store.backingStore.increaseWeight(forLinkWithLabel: predicate,
                                               between: subject,
                                               and: object) { [weak self]
            result in
            switch result {
            case .failure(let err):
                dispatch.interrupt(err)
            case .success(let newWeight):
                if let _ = self {
                    log.debug("New weight for triple [<\(self!)> <\(predicate)> <\(target)>]: \(newWeight, privacy: .public)")
                }
                dispatch.semaphore.signal()
            }
        }
        
        // When the write batch is done, propagate links based on rules
        do {
            try dispatch.wait()
            self.linkBasedOnRules(afterConnecting: target, completionHandler: completionHandler)
            self.store.delegate?.linkedDataDidChange()
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    /**
     Remove the link from this KBEntity to the one passed as argument, that matches a certain predicate label

     - parameter target: the matching object
     - parameter label: the matching predicate
     - parameter ignoreWeights: if true, removes the links regardless of their weight, otherwise decrements the weight value. Links with weight 0 will be removed
     - parameter completionHandler: the callback method

     */
    public func unlink(to target: KBEntity,
                       withPredicate label: Label,
                       ignoreWeights: Bool = false,
                       completionHandler: @escaping KBActionCompletion) {
        if ignoreWeights {
            self.store.backingStore.dropLink(withLabel: label,
                                             between: self.identifier,
                                             and: target.identifier) { [weak self] result in
                switch result {
                case .success():
                    if let _ = self {
                        log.debug("Deleted link [<\(self!)> <\(label)> <\(target)>]")
                    }
                case .failure(let err):
                    if let _ = self {
                        log.debug("Could not unlink [<\(self!)> <\(label)> <\(target)>]: \(err.localizedDescription, privacy: .public)")
                    }
                }
                completionHandler(result)
                self?.store.delegate?.linkedDataDidChange()
            }
        } else {
            self.store.backingStore.decreaseWeight(forLinkWithLabel: label,
                                                   between: self.identifier,
                                                   and: target.identifier) { [weak self] result in
                switch result {
                case .success(let newWeight):
                    if let _ = self {
                        log.debug("New weight for triple [\(self!)> <\(label)> <\(target)>]: \(newWeight, privacy: .public)")
                    }
                    completionHandler(.success(()))
                    self?.store.delegate?.linkedDataDidChange()
                case .failure(let err):
                    if let _ = self {
                        log.debug("Could not unlink [\(self!)> <\(label)> <\(target)>]: \(err.localizedDescription, privacy: .public)")
                    }
                    completionHandler(.failure(err))
                }
            }
            
        }
    }
    
    /**
     Remove the entity from the graph
     */
    public func remove(completionHandler: @escaping KBActionCompletion) {
        self.store.backingStore.dropLinks(withLabel: nil, from: self.identifier, completionHandler: completionHandler)
        self.store.delegate?.linkedDataDidChange()
    }
}


extension KBEntity {

    /**
     Returns an array of KBEntity objects `self` is connected to

     The predicate label needs to match the one passed as argument

     - parameter predicate: constraints the search to a specific predicate label
     - parameter matchType: (defaults .equal) defines the match type on the predicate label.
     Note: only .equal and .beginsWith are supported
     - parameter complement: (defaults false) if true returns the complementary set
     - parameter completionHandler: the callback method
     */
    public func linkedEntities(withPredicate predicate: Label,
                               matchType: KBMatchType = .equal,
                               complement wantsComplementarySet: Bool = false,
                               completionHandler:@escaping (Swift.Result<[(predicate: Label, object: KBEntity)], Error>) -> ()) {
        let negatedFlag = wantsComplementarySet  == true ? "NOT " : ""
        log.trace("\(negatedFlag, privacy: .public)[<\(self)> <\(predicate):\(matchType.description, privacy: .public)> $?]")

        var partial = KBHexastore.JOINER.combine(KBHexastore.SPO.rawValue, self.identifier)

        switch (wantsComplementarySet, matchType) {
        case (false, let match) where match == .equal || match == .beginsWith:
            partial = KBHexastore.JOINER.combine(partial, predicate, end: true)
        case(true, .equal):
            partial += KBHexastore.JOINER
        case(_, .beginsWith):
            break
        default:
            completionHandler(.failure(KBError.notSupported))
            return
        }

        var condition = KBTripleCondition(KBGenericCondition(.beginsWith, value: partial))
        if wantsComplementarySet  {
            let value = partial + (matchType == .beginsWith ? KBHexastore.JOINER + predicate : predicate + KBHexastore.JOINER)
            let matchesLabel = KBTripleCondition(KBGenericCondition(.beginsWith, value: value))
            condition = condition.and(not(matchesLabel))
        }

        self.store.triples(matching: condition) {
            result in
            switch result {
            case .success(let triples):
                let tuples = triples.map {
                    triple in
                    (predicate: triple.predicate, object: self.store.entity(withIdentifier: triple.object))
                }
                completionHandler(.success(tuples))
            case .failure(let err):
                completionHandler(.failure(err))
                                  
            }
        }
    }

    /**
     Returns all KBEntity objects `self` is connected to, and their labeled connections

     There can be many labeled connections between two entities,
     each having either a different predicate label, or a different target entity (object)

     - parameter completionHandler: the callback method
     */
    public func linkedEntities(completionHandler: @escaping (Swift.Result<[(predicate: Label, object: KBEntity)], Error>) -> ()) {
        log.trace("[<\(self)> $? $?]")

        let partial = KBHexastore.JOINER.combine(
            KBHexastore.SPO.rawValue,
            self.identifier,
            end: true
        )
        let condition = KBTripleCondition(KBGenericCondition(.beginsWith, value: partial))

        self.store.triples(matching: condition) {
            result in
            switch result {
            case .success(let triples):
                let tuples = triples.map {
                    triple in (predicate: triple.predicate, object: self.store.entity(withIdentifier: triple.object))
                }
                completionHandler(.success(tuples))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }

    /**
     Returns an array of KBEntity objects this KBEntity is directly reachable from

     The predicate label needs to match exactly the one passed as argument

     - parameter predicate: constraints the search to a specific predicate label
     - parameter matchType: (defaults .Equal) defines the match type
     on the predicate label
     - parameter complement: (defaults false) if true returns the complementary set
     - parameter completionHandler: the callback method
     */
    public func linkingEntities(withPredicate predicate: Label,
                                matchType: KBMatchType = .equal,
                                complement wantsComplementarySet: Bool = false,
                                completionHandler: @escaping (Swift.Result<[(subject: KBEntity, predicate: Label)], Error>) -> ()) {
        let negatedFlag = wantsComplementarySet ? "NOT " : ""
        log.trace("\(negatedFlag, privacy: .public)[$? <\(predicate):\(matchType.description, privacy: .public)> \(self)]")

        var condition: KBTripleCondition

        switch matchType {
        case .beginsWith:
            let partial = KBHexastore.JOINER.combine(
                KBHexastore.OPS.rawValue,
                self.identifier,
                predicate
            )
            let matches = KBTripleCondition(KBGenericCondition(.beginsWith, value: partial))
            if wantsComplementarySet {
                let relaxedPartial = KBHexastore.JOINER.combine(
                    KBHexastore.OPS.rawValue,
                    self.identifier,
                    end: true
                )
                let matchesRelaxed = KBTripleCondition(KBGenericCondition(
                    .beginsWith,
                    value: relaxedPartial
                ))
                condition = not(matches).and(matchesRelaxed)
            } else {
                condition = matches
            }
        case .equal:
            let matches = KBTripleCondition(
                subject: nil,
                predicate: predicate,
                object: self.identifier
            )
            if wantsComplementarySet {
                let matchesRelaxed = KBTripleCondition(
                    subject: nil,
                    predicate: nil,
                    object: self.identifier
                )
                condition = not(matches).and(matchesRelaxed)
            } else {
                condition = matches
            }
            let beginsWithOPS = KBTripleCondition(KBGenericCondition(
                .beginsWith,
                value: KBHexastore.OPS.rawValue
            ))
            condition = condition.and(beginsWithOPS)
        default:
            completionHandler(.failure(KBError.notSupported))
            return
        }
        
        self.store.triples(matching: condition) {
            result in
            switch result {
            case .success(let triples):
                let tuples = triples.map {
                    triple in (subject: self.store.entity(withIdentifier: triple.subject), predicate: triple.predicate)
                }
                completionHandler(.success(tuples))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }

    /**
     Returns all KBEntity objects this KBEntity is directly reachable from,
     and their labeled connections

     There can be many labeled connections between two entities,
     each having either a different predicate label, or a different target entity (object)
     
     - parameter completionHandler: the callback method
     */
    public func linkingEntities(completionHandler: @escaping (Swift.Result<[(subject: KBEntity, predicate: Label)], Error>) -> ()) {
        log.trace("[$? $? <\(self)>]")

        let partial = KBHexastore.JOINER.combine(
            KBHexastore.OPS.rawValue,
            self.identifier,
            end: true
        )
        let condition = KBTripleCondition(KBGenericCondition(.beginsWith, value: partial))
            
        self.store.triples(matching: condition) {
            result in
            switch result {
            case .success(let triples):
                let tuples = triples.map {
                    triple in (subject: self.store.entity(withIdentifier: triple.subject), predicate: triple.predicate)
                }
                completionHandler(.success(tuples))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }

    // MARK: Links

    /**
     Returns all the predicate labels connecting `this` KBEntity,
     and the one passed as argument

     - parameter target: constraints the query to a particular KBEntity
     - parameter matchType: (defaults .Equal) defines the match type
     on the identifier of the linked KBEntity (target)
     - parameter completionHandler: the callback method
     */
    public func links(to target: KBEntity,
                      matchType: KBMatchType = .equal,
                      completionHandler: @escaping (Swift.Result<[Label], Error>) -> ()) {
        log.trace("[<\(self)> $? <\(target):\(matchType.description, privacy: .public)>]")

        let partial: String

        switch matchType {
        case .beginsWith:
            partial = KBHexastore.JOINER.combine(
                KBHexastore.SOP.rawValue,
                self.identifier,
                target.identifier
            )
        case .equal:
            partial = KBHexastore.JOINER.combine(
                KBHexastore.SOP.rawValue,
                self.identifier,
                target.identifier,
                end: true
            )
        default:
            completionHandler(.failure(KBError.notSupported))
            return
        }

        let condition = KBTripleCondition(KBGenericCondition(.beginsWith, value: partial))
        
        self.store.triples(matching: condition) {
            result in
            switch result {
            case .success(let triples):
                let tuples = triples.map {
                    triple in triple.predicate
                }
                completionHandler(.success(tuples))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
}

extension KBEntity {
    
    
    /// Check if any KBLogic is satisfied.
    /// If so, infer the links or execute KBExecutableClosure behavior as defined by the rule.
    /// See KBKnowledgeStore::inferLink and KBKnowledgeStore::executeBehavior
    /// - Parameters:
    ///   - target: the entity being linked to this entity
    ///   - completionHandler: the completion handler
    internal func linkBasedOnRules(afterConnecting target: KBEntity, completionHandler: @escaping KBActionCompletion) {
        let queue = DispatchQueue(label: "\(KnowledgeBaseBundleIdentifier).KBEntity.rules")
        
        self.satisfiableRules(afterConnecting: target, usingQueue: queue) { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let satisfiableRules):
                if satisfiableRules.count == 0 {
                    completionHandler(.success(()))
                    return
                }
                
                let dispatch = KBTimedDispatch()
                
                for rule in satisfiableRules {
                    dispatch.group.enter()
                    if rule.predicate.beginsWith(CLOSURE_PREFIX) {
    //                    assert(rule.predicate.endIndex > CLOSURE_PREFIX.endIndex)
    //                    let closureIdentifier = rule.predicate.substring(from: CLOSURE_PREFIX.endIndex)
    //
    //                    if let data = self.store._value(forKey: closureIdentifier) as? Data {
    //                        if let closure = NSKeyedUnarchiver.unarchiveObject(with: data)
    //                            as? KBExecutableClosure {
    //                            log.debug("executing behavior \(closure.identifier, privacy: .public) fired by linking [<\(self)> <\(rule.predicate)> <\(target)>]")
    //                            closure.execute()
    //                        } else {
    //                            log.error("bad data for \(closure.identifier, privacy: .public)")
    //                            dispatch.interrupt(KBError.unexpectedData)
    //                        }
    //                        dispatch.interrupt(KBError.notSupported)
    //                    }
                        dispatch.group.leave()
                    } else {
                        self.link(to: rule.object, withPredicate: rule.predicate) { result in
                            switch result {
                            case .failure(let err):
                                dispatch.interrupt(err)
                            case .success(()):
                                dispatch.group.leave()
                            }
                        }
                    }
                }
                
                do {
                    try dispatch.wait()
                    completionHandler(.success(()))
                } catch {
                    completionHandler(.failure(error))
                }
            }
        }
    }
    
    private func nonNegatedRuleComponentsAreSatisfied(inRule ruleEntity: KBEntity,
                                                      usingQueue: DispatchQueue,
                                                      completionHandler: @escaping (Swift.Result<Bool, Error>) -> ()) {
        ruleEntity.linkingEntities(withPredicate: NEGATION_PREFIX, matchType: .beginsWith, complement: true) {
            result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let nonNegatedRuleComponents):
                if nonNegatedRuleComponents.count == 0 {
                    completionHandler(.success(true))
                    return
                }
                
                var ruleSatisfied = true
                let dispatch = KBTimedDispatch(timeout: .now() + .seconds(nonNegatedRuleComponents.count))
                
                for ruleBody in nonNegatedRuleComponents {
                    let existsCondition = KBTripleCondition(
                        subject: self.identifier,
                        predicate: ruleBody.predicate,
                        object: ruleBody.subject.identifier
                    )
                    
                    dispatch.group.enter()
                    self.store.triples(matching: existsCondition) { result in
                        switch result {
                        case .failure(let err):
                            dispatch.interrupt(err)
                        case .success(let triples):
                            if triples.count == 0 {
                                ruleSatisfied = false
                                dispatch.semaphore.signal()
                            } else {
                                dispatch.group.leave()
                            }
                        }
                    }
                }
                
                do {
                    try dispatch.wait()
                    completionHandler(.success(ruleSatisfied))
                } catch {
                    completionHandler(.failure(error))
                }
            }
        }
    }
    
    private func negatedRuleComponentsAreSatisfied(inRule ruleEntity: KBEntity,
                                                   usingQueue: DispatchQueue,
                                                   completionHandler: @escaping (Swift.Result<Bool, Error>) -> ()) {
        ruleEntity.linkingEntities(withPredicate: NEGATION_PREFIX,
                                   matchType: .beginsWith) { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let negatedRuleComponents):
                if negatedRuleComponents.count == 0 {
                    completionHandler(.success(true))
                    return
                }
                
                var ruleSatisfied = true
                let dispatch = KBTimedDispatch(timeout: .now() + .seconds(negatedRuleComponents.count * 10))
                
                for negatedRuleBody in negatedRuleComponents {
                    assert(negatedRuleBody.predicate.beginsWith(NEGATION_PREFIX))
                    assert(negatedRuleBody.predicate.endIndex > NEGATION_PREFIX.endIndex)
                    let predicate = String(negatedRuleBody.predicate[NEGATION_PREFIX.endIndex...])
                    
                    let notExistsCondition = KBTripleCondition(
                        subject: self.identifier,
                        predicate: predicate,
                        object: negatedRuleBody.subject.identifier
                    )
                    dispatch.group.enter()
                    self.store.triples(matching: notExistsCondition) { result in
                        switch result {
                        case .failure(let err):
                            dispatch.interrupt(err)
                        case .success(let triples):
                            if triples.count > 0 {
                                ruleSatisfied = false
                                dispatch.semaphore.signal()
                            } else {
                                dispatch.group.leave()
                            }
                        }
                    }
                }
                
                do {
                    try dispatch.wait()
                    completionHandler(.success(ruleSatisfied))
                } catch {
                    completionHandler(.failure(error))
                }
            }
        }
    }
    
    /**
     Get all the triples having subject this KBEntity and predicate
     WHERE [subject=self.identifier, predicate="$(NEGATION_PREFIX)?$(RULE_PREFIX)*", object=?]
     GROUP BY predicate, object
     
     - parameter afterConnecting: the entity `this` has just been connected to
     - parameter completionHandler: the callback method
     */
    private func satisfiableRules(afterConnecting entity: KBEntity,
                                  usingQueue queue: DispatchQueue,
                                  completionHandler: @escaping (Swift.Result<[(predicate: Label, object: KBEntity)], Error>) -> ()) {
        var inferredLinks = [(predicate: Label, object: KBEntity)]()
        let allRulesFromTarget = KBTripleCondition.forRules(from: entity)
        
        self.store.triples(matching: allRulesFromTarget) { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let triplesFromTarget):
                // FIXME: This filter should be done on DB
                let ruleTriplesFromTarget = triplesFromTarget.filter {
                    $0.object.beginsWith(RULE_PREFIX) ||
                        $0.object.beginsWith(NEGATION_PREFIX, RULE_PREFIX)
                }
                
                if ruleTriplesFromTarget.count == 0 {
                    completionHandler(.success([]))
                    return
                }
                
                let dispatch = KBTimedDispatch(timeout: .now() + .seconds(ruleTriplesFromTarget.count * 100))
                
                // For each rule …
                for rule in ruleTriplesFromTarget {
                    let ruleEntity = self.store.entity(withIdentifier: rule.object)
                    
                    // … check all the REQUIRED links are there …
                    dispatch.group.enter()
                    self.nonNegatedRuleComponentsAreSatisfied(inRule: ruleEntity,
                                                              usingQueue: queue) { result in
                        switch result {
                        case .failure(let err):
                            dispatch.interrupt(err)
                        case .success(let satisfied):
                            // … and terminate early if some is missing!
                            if !satisfied {
                                dispatch.group.leave()
                                return
                            }
                            
                            // … then check all the NOT condition of the rule are satisfied (links are NOT there) …
                            self.negatedRuleComponentsAreSatisfied(inRule: ruleEntity,
                                                                   usingQueue: queue) { result in
                                switch result {
                                case .failure(let err):
                                    dispatch.interrupt(err)
                                case .success(let satisfied):
                                    // … and terminate early if there is some!
                                    if !satisfied {
                                        dispatch.group.leave()
                                        return
                                    }
                                    
                                    // Now if the rule is satisfied, retrieve all the links to infer
                                    ruleEntity.linkedEntities() { result in
                                        switch result {
                                        case .failure(let err):
                                            dispatch.interrupt(err)
                                        case .success(let linkedEntities):
                                            inferredLinks += linkedEntities
                                            dispatch.group.leave()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                do {
                    try dispatch.wait()
                    completionHandler(.success(inferredLinks))
                } catch {
                    completionHandler(.failure(error))
                }
            }
        }
    }
}
