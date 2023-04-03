//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation

public typealias Label = String

internal typealias Tuple = (Label, Label, Label, Int)

// MARK: - Triple

let _KBTriple_subject = "subject"
let _KBTriple_predicate = "predicate"
let _KBTriple_object = "object"
let _KBTriple_weight = "weight"

@objc(KBTriple)
public class KBTriple : NSObject, NSSecureCoding {
    fileprivate let value: Tuple
    
    @objc public var subject: Label { return self.value.0 }
    @objc public var predicate: Label { return self.value.1 }
    @objc public var object: Label { return self.value.2 }
    @objc public var weight: Int { return self.value.3 }
    
    @objc public override var description: String {
        return "{\(self.subject), \(self.predicate), \(self.object)}[\(self.weight)]"
    }

    @objc public override var hash: Int {
        return self.subject.hashValue ^
            self.predicate.hashValue ^
            self.object.hashValue
    }
    
    @objc public override func isEqual(_ object: Any?) -> Bool {
        if let rhs = object as? KBTriple {
            return self == rhs
        }
        return super.isEqual(object)
    }
    
    internal init(tuple: Tuple) throws {
        self.value = tuple
    }

    @objc public init(subject: Label, predicate: Label, object: Label, weight: Int) {
        self.value = (subject, predicate, object, weight)
    }
    
    public func encode(with aCoder: NSCoder) {
        aCoder.encode(self.subject, forKey: _KBTriple_subject)
        aCoder.encode(self.predicate, forKey: _KBTriple_predicate)
        aCoder.encode(self.object, forKey: _KBTriple_object)
        aCoder.encode(self.weight, forKey: _KBTriple_weight)
    }
    
    public required convenience init?(coder aDecoder: NSCoder) {
        let subj = aDecoder.decodeObject(of: NSString.self, forKey: _KBTriple_subject)
        let pred = aDecoder.decodeObject(of: NSString.self, forKey: _KBTriple_predicate)
        let obj = aDecoder.decodeObject(of: NSString.self, forKey: _KBTriple_object)
        let weight = aDecoder.decodeInteger(forKey: _KBTriple_weight)
        
        guard let subject = subj as Label? else {
            log.error("unexpected value for subject when decoding KBTriple object")
            return nil
        }
        guard let predicate = pred as Label? else {
            log.error("unexpected value for predicate when decoding KBTriple object")
            return nil
        }
        guard let object = obj as Label? else {
            log.error("unexpected value for object when decoding KBTriple object")
            return nil
        }
        
        self.init(subject: subject,
                  predicate: predicate,
                  object: object,
                  weight: weight)
    }
    
    public static var supportsSecureCoding: Bool {
        return true
    }

    internal func dictionary() -> [String: Any] {
        return ["subject": self.subject,
                "predicate": self.predicate,
                "object": self.object,
                "weight": self.weight]
    }
}

public func ==(lhs: KBTriple, rhs: KBTriple) -> Bool {
    return lhs.subject == rhs.subject
        && lhs.predicate == rhs.predicate
        && lhs.object == rhs.object
}
