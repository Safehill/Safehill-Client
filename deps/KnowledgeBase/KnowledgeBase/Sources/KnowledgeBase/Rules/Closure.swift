//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/28/21.
//

import Foundation


public protocol KBExecutableClosure : NSCoding {
    var identifier: String { get }
    func execute()
}

@objc(KBClosure)
open class KBClosure : NSObject, KBExecutableClosure {
    let store: KBKnowledgeStore
    let entity: KBEntity

    final public var identifier: String {
        return "\(UUID().uuidString)"
    }

    required public init?(coder aDecoder: NSCoder) {
        guard let location = aDecoder.decodeObject(forKey: "location") as? String else {
            log.error("unable to decode location in KBClosure initializer")
            return nil
        }
        guard let entityIdentifier = aDecoder.decodeObject(forKey: "entityIdentifier") as? String else {
            log.error("unable to decode entityIdentifier in KBClosure initializer")
            return nil
        }

        do {
            let location = try KBKnowledgeStore.Location.decode(location)
            self.store = KBKnowledgeStore.store(location) as! KBKnowledgeStore
        } catch {
            log.error("error extracting store location. \(error.localizedDescription, privacy: .public)")
            return nil
        }
        self.entity = self.store.entity(withIdentifier: entityIdentifier)
    }

    @objc(encodeWithCoder:) open func encode(with aCoder: NSCoder) {
        aCoder.encode(self.store.location.encoded, forKey: "location")
        aCoder.encode(self.entity.identifier, forKey: "entityIdentifier")
    }

    /**
     Executes the behavior stored with *Self* identifier when the KBRuleBasedLogic applies
     */
    open func execute() {
        /* TODO: What happens if a closure is linked to a closure? Chained behavior? */
    }
}

public extension KBKnowledgeStore {
    /**
     Store an inference rule in this KBKnowledgeStore, that triggers, from this point on,
     a call to the *execute()* method of the input KBExecutableClosure whenever *any* KBEntity object
     that gets linked to other KBEntity objects, satisfies the rule passed as parameter.

     KBExecutableClosure and Inference rules are represented in the KBKnowledgeStore as KBEntity objects.
     The latter have:
     - as many linking entities as how many body (positive and negative) literals are defined in the rule
     - one linked entity, namely the KBExecutableClosure

     - Parameters:
     - closure: The KBExecutableClosure to execute when the *rule* is satisfied
     - rule: The *rule* to be satisfied
     - completionHandler: the callback method

     - Throws: KBRuleBasedLogicError.MissingRuleBody
     */
    func execute(behavior closure: KBExecutableClosure,
                 when rule: KBRuleBasedLogic) async throws {
        guard rule.body.count + rule.negatedBody.count > 0 else {
            throw KBRuleBasedLogicError.missingRuleBody
        }
        
        let ruleEntity = self.entity(withIdentifier: rule.identifier)
        let closureEntity = self.entity(withIdentifier: closure.identifier)

        log.info("will execute behavior with identifier \(closure.identifier, privacy: .private(mask: .hash)) every time \(rule.body, privacy: .public)")
        try await ruleEntity.link(to: closureEntity, withPredicate: "\(CLOSURE_PREFIX)\(closure)")
        
        for ruleLiteral in rule.body {
            try await ruleLiteral.object.link(to: ruleEntity,
                                              withPredicate: ruleLiteral.predicate ?? "*")
        }
        
        for ruleLiteral in rule.negatedBody {
            try await ruleLiteral.object.link(to: ruleEntity,
                                              withPredicate: "\(NEGATION_PREFIX)\(ruleLiteral.predicate ?? "*")")
        }
        
        let value: Any
        if #available(macOS 10.13, *) {
            value = try NSKeyedArchiver.archivedData(withRootObject: closure, requiringSecureCoding: true)
        } else {
            value = NSKeyedArchiver.archivedData(withRootObject: closure)
        }
        try await self.set(value: value, for: closure.identifier)
    }
}
