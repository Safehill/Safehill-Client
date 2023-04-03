//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/28/21.
//

import Foundation

enum KBRuleBasedLogicError: Error {
    case missingRuleBody
    case unsatisfiableRule
}


typealias RuleLiteral = (predicate: Label?, object: KBEntity)

@objc(KBRuleBasedLogic)
open class KBRuleBasedLogic : NSObject {
    internal var body: [RuleLiteral]
    internal var negatedBody: [RuleLiteral]

    @objc open override var hash: Int {
        get {
            return (body.reduce("") { "\($0)-\($1.object).\($1.predicate ?? "")" }
                + negatedBody.reduce("") { "\($0)-\($1.object).\($1.predicate ?? "")" }).hashValue
        }
    }

    @objc open var identifier: String {
        return "\(RULE_PREFIX)\(String(self.hash))"
    }

    fileprivate init(body: [RuleLiteral] = [], negatedBody: [RuleLiteral] = []) {
        self.body = body
        self.negatedBody = negatedBody
    }

    @objc open class func ifExistsLink(_ predicate: Label?, to: KBEntity) -> KBRuleBasedLogic {
        return KBRuleBasedLogic(body: [(predicate: predicate, object: to)])
    }

    @objc open func andExistsLink(_ predicate: Label?, to: KBEntity) throws -> KBRuleBasedLogic {
        let inNegatedBody = self.negatedBody.filter { predicate == $0.predicate && to == $0.object }
        if inNegatedBody.count > 0 {
            throw KBRuleBasedLogicError.unsatisfiableRule
        }
        self.body.append((predicate: predicate, object: to))
        return self
    }

    @objc open class func ifNotExistsLink(_ predicate: Label?, to: KBEntity) -> KBRuleBasedLogic {
        return KBRuleBasedLogic(negatedBody: [(predicate: predicate, object: to)])
    }

    @objc open func andNotExistsLink(_ predicate: Label?, to: KBEntity) throws -> KBRuleBasedLogic {
        let inPositiveBody = self.body.filter { predicate == $0.predicate && to == $0.object }
        if inPositiveBody.count > 0 {
            throw KBRuleBasedLogicError.unsatisfiableRule
        }
        self.negatedBody.append((predicate: predicate, object: to))
        return self
    }

    @objc open func and(_ rule: KBRuleBasedLogic) -> KBRuleBasedLogic {
        return KBRuleBasedLogic(
            body: self.body + rule.body,
            negatedBody: self.negatedBody + rule.negatedBody
        )
    }
}

public extension KBKnowledgeStore {
    
    /**
     Store an inference rule in this CKKnowledgeStore, that triggers, from this point on, the creation
     of extra links (as defined by *links*) from *any* KBEntity object that gets linked to objects
     of the same type in the graph, and that satisfies the rule.
     
     Inference rules are represented in the CKKnowledgeStore as KBEntity objects
     and that have:
     - as many linking entities as how many body (positive and negative) literals are defined in the rule
     - linked entities as defined by the tuples (predicate, object) passed as argument
     
     Thus, the  extra links are then connected to the *rule-entity* in the CKKnowledgeStore, to not interfere
     with other connections.
     
     - Parameters:
     - to: the object to infer when the *rule* is satisfied
     - predicate: the predicate to infer when the *rule* is satisfied
     - when: The *rule* to be satisfied
     - completionHandler: the callback method
     
     */
    @objc func inferLink(to linkedEntitiy: KBEntity, withPredicate predicate: Label, when rule: KBRuleBasedLogic) async throws {
        guard rule.body.count + rule.negatedBody.count > 0 else {
            throw KBRuleBasedLogicError.missingRuleBody
        }
        
        let ruleEntity = self.entity(withIdentifier: rule.identifier)

        log.info("will infer link \(predicate) to \(linkedEntity) every time \(rule.body)")
        
        try await ruleEntity.link(to: linkedEntitiy, withPredicate: predicate)
        
        if rule.body.count > 0 {
            for ruleLiteral in rule.body {
                try await ruleLiteral.object.link(to: ruleEntity, withPredicate: "\(NEGATION_PREFIX)\(ruleLiteral.predicate ?? "*")")
            }
        }
        
        if rule.negatedBody.count > 0 {
            for ruleLiteral in rule.negatedBody {
                try await ruleLiteral.object.link(to: ruleEntity, withPredicate: ruleLiteral.predicate ?? "*")
            }
        }
    }
}
