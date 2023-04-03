//
//  Entity.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation

let RULE_PREFIX = "rule-"
let NEGATION_PREFIX = "!!!-"
let CLOSURE_PREFIX = "closure-"


@objc(KBEntity)
public class KBEntity : NSObject {
    public let identifier: Label
    let store: KBKnowledgeStore

    internal init(identifier id: Label, knowledgeStore: KBKnowledgeStore) {
        self.identifier = id
        self.store = knowledgeStore
    }

    // MARK: Hashable, Equatable protocol

    @objc public override func isEqual(_ object: Any?) -> Bool {
        if let object = object as? KBEntity {
            return self.store.isEqual(object.store)
                && self.identifier == object.identifier
        }
        return false
    }
    
    @objc public override var hash: Int {
        return (self.store.name.hashValue
            ^ self.identifier.hashValue);
    }

    // MARK: CustomStringConvertible protocol

    @objc public override var description: String {
        return self.identifier
    }
}

public func ==(lhs: KBEntity, rhs: KBEntity) -> Bool {
    return lhs.identifier == rhs.identifier
}

public func ==<T: KBEntity>(lhs: T, rhs: T) -> Bool {
    return lhs.identifier == rhs.identifier
}
