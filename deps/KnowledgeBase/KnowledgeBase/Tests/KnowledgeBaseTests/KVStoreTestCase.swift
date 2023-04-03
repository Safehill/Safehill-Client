//
//  KVStoreTestCase.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/11/21.
//

import XCTest
@testable import KnowledgeBase

class KVStoreTestCase : XCTestCase {

    private static let _sharedStore = KBKVStore.store(.inMemory)
    
    func sharedStore() -> KBKVStore {
        return KVStoreTestCase._sharedStore
    }

    private func cleanup() {
        do {
            let store = self.sharedStore()
            let _ = try store.removeAll()
            let keys = try store.keys()
            XCTAssert(keys.count == 0, "Removed all values")
        } catch {
            XCTFail("\(error)")
        }
    }

    override func setUp() {
        self.cleanup()
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
        self.cleanup()
    }

    func isEqual(_ a: Any, _ b: Any) -> Bool {
        if var a = a as? Array<Any> {
            guard var b = b as? Array<Any> else { return false }
            guard b.count == a.count else { return false }
            if let aS = a as? Array<String>, let bS = a as? Array<String> {
                a = aS.sorted()
                b = bS.sorted()
            }
            if let aS = a as? Array<Dictionary<String, Any>>, let bS = a as? Array<Dictionary<String, Any>> {
                let sortCriteria = {
                    (x: Dictionary<String, Any>, y: Dictionary<String, Any>) in
                    String(describing: x["@id"]) < String(describing: y["@id"])
                }
                a = aS.sorted(by: sortCriteria)
                b = bS.sorted(by: sortCriteria)
            }
            for (idx, entity) in a.enumerated() {
                if (!self.isEqual(entity, b[idx])) {
                    return false
                }
            }
            return true
        } else if let a = a as? Dictionary<String, Any> {
            guard let b = b as? Dictionary<String, Any> else { return false }
            for (k, v) in a {
                if (!self.isEqual(v, b[k]!)) {
                    return false
                }
            }
            return true
        } else if let a = a as? String {
            guard let b = b as? String else { return false }

            return (a == b)
        } else if let a = a as? Int {
            guard let b = b as? Int else { return false }

            return (a == b)
        } else if let a = a as? Double {
            guard let b = b as? Double else { return false }

            return (a == b)
        }

        print("\(#function): Cannot compare \(a) and \(b)")
        return false
    }
    
    func testPartialKeysAndValues() {
        do {
            try KVStoreTestCase._sharedStore.set(value: "stringVal", for: "string")
            print(try KVStoreTestCase._sharedStore.dictionaryRepresentation())
            try KVStoreTestCase._sharedStore.set(value: 1, for: "int")
            try KVStoreTestCase._sharedStore.set(value: true, for: "bool")
            try KVStoreTestCase._sharedStore.set(value: false, for: "NOTbool")
            try KVStoreTestCase._sharedStore.set(value: ["first", "second"], for: "array")
            try KVStoreTestCase._sharedStore.set(value: ["first": "first", "second": "second"], for: "dictionary")
            
            let stringOrBool = KBGenericCondition(.equal, value: "string").or(KBGenericCondition(.equal, value: "bool"))

            var partialKeysAndValues = try KVStoreTestCase._sharedStore.dictionaryRepresentation(forKeysMatching: stringOrBool) as Dictionary
            XCTAssertEqual(partialKeysAndValues.keys.count, 2)
            XCTAssertEqual(partialKeysAndValues["string"] as? String, "stringVal")
             XCTAssertEqual(partialKeysAndValues["bool"] as? Bool, true)

            var partialKeys = try KVStoreTestCase._sharedStore.keys(matching: stringOrBool).sorted { $0.compare($1) == .orderedAscending }
            XCTAssert(partialKeys.count == 2)
            XCTAssertEqual(partialKeys, ["bool", "string"])

            var partialValues = try KVStoreTestCase._sharedStore.values(for: partialKeys) as [Any?]
            XCTAssert(partialValues.count == 2)

            partialValues = try KVStoreTestCase._sharedStore.values(forKeysMatching: stringOrBool)
            XCTAssert(partialValues.count == 2)

            let startWithSOrEndsWithOl = KBGenericCondition(.beginsWith, value: "s").or(KBGenericCondition(.endsWith, value: "ol"))
            partialKeysAndValues = try KVStoreTestCase._sharedStore.dictionaryRepresentation(forKeysMatching: startWithSOrEndsWithOl) as Dictionary
            XCTAssertEqual(partialKeysAndValues.keys.count, 3)
            XCTAssertEqual(partialKeysAndValues["string"] as? String, "stringVal")
            // TODO: rdar://50960552
            // XCTAssertEqual(partialKeysAndValues["bool"] as? Bool, true)
            // XCTAssertEqual(partialKeysAndValues["NOTbool"] as? Bool, false)

            partialKeys = try KVStoreTestCase._sharedStore.keys(matching: startWithSOrEndsWithOl).sorted { $0.compare($1) == .orderedAscending }
            XCTAssertEqual(partialKeys.count, 3)
            XCTAssertEqual(partialKeys, ["NOTbool", "bool", "string"])

            partialValues = try KVStoreTestCase._sharedStore.values(for: partialKeys) as [Any?]
            XCTAssertEqual(partialValues.count, 3)

            partialValues = try KVStoreTestCase._sharedStore.values(forKeysMatching: startWithSOrEndsWithOl)
            XCTAssertEqual(partialValues.count, 3)
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testAllKeyValues() throws {
        try KVStoreTestCase._sharedStore.set(value: "stringVal", for: "string")
        try KVStoreTestCase._sharedStore.set(value: 1, for: "int")
        try KVStoreTestCase._sharedStore.set(value: true, for: "bool")
        try KVStoreTestCase._sharedStore.set(value: false, for: "NOTbool")
        try KVStoreTestCase._sharedStore.set(value: ["first", "second"], for: "array")
        try KVStoreTestCase._sharedStore.set(value: ["first": "first", "second": "second"], for: "dictionary")
        
        let kvPairs = try KVStoreTestCase._sharedStore.dictionaryRepresentation() as Dictionary
        XCTAssertEqual(kvPairs.count, 6)
        XCTAssertEqual(kvPairs["string"] as? String, "stringVal")
        XCTAssertEqual(kvPairs["int"] as? Int, 1)
        XCTAssertEqual(kvPairs["bool"] as? Bool, true)
        XCTAssertEqual(kvPairs["NOTbool"] as? Bool, false)

        let arrayValue = kvPairs["array"] as! [String]
        XCTAssertEqual(arrayValue.count, 2)
        XCTAssertEqual(arrayValue[0], "first")
        XCTAssertEqual(arrayValue[1], "second")

        let dictValue = kvPairs["dictionary"] as! [String:String]
        XCTAssertEqual(dictValue.count, 2)
        XCTAssertEqual(dictValue["first"], "first")
        XCTAssertEqual(dictValue["second"], "second")

        let keys = try KVStoreTestCase._sharedStore
            .keys(matching: KBGenericCondition(value: true))
            .sorted { $0.compare($1) == .orderedAscending }

        XCTAssertEqual(keys, ["NOTbool", "array", "bool", "dictionary", "int", "string"])

        let keyedValues = try KVStoreTestCase._sharedStore.values(for: keys)
        XCTAssertEqual(keyedValues.count, 6)

        let conditionalValues = try KVStoreTestCase._sharedStore.values(forKeysMatching: KBGenericCondition(value: true))
        XCTAssertEqual(conditionalValues.count, 6)
    }
    
    struct NonNSSecureCodingCompliantStruct {
    }
    
    class NonNSSecureCodingCompliantClass {
    }
    
    func testSetValueForKeyUnsecure() throws {
        let key = "NonNSSecureCodingCompliant"
        let emptyString = ""

        for nonSecureValue in [NonNSSecureCodingCompliantStruct(), NonNSSecureCodingCompliantClass()] as [Any] {
            try KVStoreTestCase._sharedStore.set(value: emptyString, for: key)
            
            let stringValue = try KVStoreTestCase._sharedStore.value(for: key)
            XCTAssertNotNil(stringValue as? String)
            XCTAssert((stringValue as? String) == emptyString)
            
            try KVStoreTestCase._sharedStore.set(value: nonSecureValue, for: key)
            let invalidValue = try KVStoreTestCase._sharedStore.value(for: key)
            XCTAssertNotNil(invalidValue as? String)
            XCTAssert((invalidValue as? String) == emptyString)
            
            do { try KVStoreTestCase._sharedStore.removeValue(for: key) } catch { XCTFail() }
            let removedValue = try KVStoreTestCase._sharedStore.value(for: key)
            XCTAssertNil(removedValue)
            
            try KVStoreTestCase._sharedStore.set(value: nonSecureValue, for: key)
            let invalidValue2 = try KVStoreTestCase._sharedStore.value(for: key)
            XCTAssertNil(invalidValue2)
        }
        
        let triple = KBTriple(subject: "Luca", predicate: "is", object: "awesome", weight: 1)
        try KVStoreTestCase._sharedStore.set(value: triple, for: key)
        let tripleValue = try KVStoreTestCase._sharedStore.value(for: key)
        XCTAssertNotNil(tripleValue)
        XCTAssert((tripleValue as? KBTriple) == triple)
    }

    func testWriteBatch() throws {
        let writeBatch = KVStoreTestCase._sharedStore.writeBatch()
        writeBatch.set(value: "stringVal", for: "string")
        writeBatch.set(value: 1, for: "int")
        writeBatch.set(value: true, for: "bool")
        writeBatch.set(value: false, for: "NOTbool")
        writeBatch.set(value: ["first", "second"], for: "array")
        writeBatch.set(value: ["first": "first", "second": "second"], for: "dictionary")
        do {
            try writeBatch.write()
        } catch {
            XCTFail("\(error)")
        }

        let none = try KVStoreTestCase._sharedStore.value(for: "none")
        XCTAssert(none == nil, "non existing attribute")

        let string = try KVStoreTestCase._sharedStore.value(for: "string")
        XCTAssert(string != nil, "string exists")
        XCTAssert((string as? String) != nil, "string is a string")
        XCTAssert((string as? String) == "stringVal", "string value matches")

        let int = try KVStoreTestCase._sharedStore.value(for: "int")
        XCTAssert(int != nil, "int exists")
        XCTAssert((int as? Int) != nil, "int is an int")
        XCTAssert((int as? Int) == 1, "int value matches")

        let bool = try KVStoreTestCase._sharedStore.value(for: "bool")
        XCTAssert(bool != nil, "bool exists")
        XCTAssert((bool as? Bool) != nil, "bool is a bool")
        XCTAssert((bool as? Bool) == true, "bool value matches")
        let notbool = try KVStoreTestCase._sharedStore.value(for: "NOTbool")
        XCTAssert((notbool as? Bool) != nil, "NOTbool is a bool")
        XCTAssert((notbool as? Bool) == false, "NOTbool value matches")

        let array = try KVStoreTestCase._sharedStore.value(for: "array")
        XCTAssert(array != nil, "array exists")
        XCTAssert((array as? Array<String>) != nil, "array is an array")
        if let array = array as? Array<String> {
            XCTAssert(array.count == 2, "array count matches")
            XCTAssert(array == ["first", "second"], "array values match")
        }

        var dict = try KVStoreTestCase._sharedStore.value(for: "dictionary")
        XCTAssert(dict != nil, "dictionary exists")
        XCTAssert((dict as? Dictionary<String, String>) != nil, "dict is a dictionary")
        if let dict = array as? Dictionary<String, String> {
            XCTAssert(dict.keys.count == 2, "dictionary keys count matches")
            XCTAssert(dict["first"] == "first", "dictionary first value matches")
            XCTAssert(dict["second"] == "second", "dictionary second value matches")
        }
        
        try KVStoreTestCase._sharedStore.set(value: nil, for: "dictionary")
        dict = try KVStoreTestCase._sharedStore.value(for: "dictionary")
        XCTAssert(dict == nil, "dictionary has been removed")
        
        writeBatch.set(value: nil, for: "bool")
        writeBatch.set(value: nil, for: "NOTbool")
        do {
            try writeBatch.write()
        } catch {
            XCTFail("\(error)")
        }
        let b = try KVStoreTestCase._sharedStore.value(for: "bool")
        XCTAssertNil(b)
        let nb = try KVStoreTestCase._sharedStore.value(for: "NOTbool")
        XCTAssertNil(nb)
    }
}

class KnowledgeStoreTestCase : KVStoreTestCase {
    
    private static let _sharedStore = KBKnowledgeStore.store(.inMemory)
    
    override func sharedStore() -> KBKnowledgeStore {
        return KnowledgeStoreTestCase._sharedStore
    }
    
    // Test times out. It tests unused, unmaintained code.
    func testLinkUnlink() {
        do {
            let subject = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "subject")
            let predicate = "predicate"
            let object = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "object")

            try subject.link(to: object, withPredicate: predicate)

            var hexaCount = try KnowledgeStoreTestCase._sharedStore.keys().count
            XCTAssert(hexaCount == 6, "1 hexatuple. \(hexaCount) tuples")

            let condition = KBTripleCondition(subject: subject.identifier,
                                              predicate: predicate,
                                              object: object.identifier)
            var triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)
            XCTAssert(triples.count == 1, "one triple. \(triples.count)")
            XCTAssert(triples[0].weight == 1, "weight is 1. \(triples[0].weight)")

            try subject.unlink(to: object, withPredicate: predicate)

            hexaCount = try KnowledgeStoreTestCase._sharedStore.keys().count
            XCTAssert(hexaCount == 0, "Tuples removed")

            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)
            XCTAssert(triples.count == 0, "no triples. \(triples.count)")

            try subject.link(to: object, withPredicate: predicate)
            try subject.link(to: object, withPredicate: predicate)

            hexaCount = try KnowledgeStoreTestCase._sharedStore.keys().count
            XCTAssert(hexaCount == 6, "1 hexatuple. \(hexaCount) tuples")

            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)
            XCTAssert(triples.count == 1, "no triples. \(triples.count)")
            XCTAssert(triples[0].weight == 2, "weight is 2. \(triples[0].weight)")

            try subject.unlink(to: object, withPredicate: predicate)

            hexaCount = try KnowledgeStoreTestCase._sharedStore.keys().count
            XCTAssert(hexaCount == 6, "1 hexatuple. \(hexaCount) tuples")

            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)
            XCTAssert(triples.count == 1, "no triples. \(triples.count)")
            XCTAssert(triples[0].weight == 1, "weight is 1. \(triples[0].weight)")
            
            try KnowledgeStoreTestCase._sharedStore.backingStore.setWeight(forLinkWithLabel: predicate,
                                                                                   between: subject.identifier,
                                                                                   and: object.identifier,
                                                                                   toValue: 4)
            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)
            XCTAssert(triples.count == 1, "no triples. \(triples.count)")
            XCTAssert(triples[0].weight == 4, "weight is 4. \(triples[0].weight)")

        } catch {
            XCTFail("\(error)")
        }
    }

    // Test fails. It test unused, unmaintained code.
    func testDuplicateLinkingWithCompletionHandler() {
        do {
            var triples: [KBTriple]
            let subject = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "subject")
            let predicate = "predicate"
            let object = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "object")

            try subject.link(to: object, withPredicate: predicate)
            try subject.link(to: object, withPredicate: predicate)
            try subject.link(to: object, withPredicate: predicate)
            try subject.link(to: object, withPredicate: predicate)

            let condition = KBTripleCondition(subject: subject.identifier,
                                              predicate: predicate,
                                              object: object.identifier)
            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)

            var hexaCount = try KnowledgeStoreTestCase._sharedStore.keys().count
            XCTAssert(hexaCount == 6, "Insert first triple once results in 1 hexatuple. \(hexaCount)")
            XCTAssert(triples.count == 1, "one triple. \(triples.count)")
            XCTAssert(triples[0].weight == 4, "weight is 4. \(triples[0].weight)")

            print(triples[0].weight)

            try subject.unlink(to: object, withPredicate: predicate)
            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)
            print(triples)

            hexaCount = try KnowledgeStoreTestCase._sharedStore.keys().count
            XCTAssert(hexaCount == 6, "still 1 hexatuple. \(hexaCount)")
            XCTAssert(triples.count == 1, "one triple. \(triples.count)")
            XCTAssert(triples[0].weight == 3, "weight is 3. \(triples[0].weight)")
            print(triples[0].weight)
        } catch {
            XCTFail("\(error)")
        }
    }

    func testSingleLinking() {
        do {
            var sp_pairs: [(subject: KBEntity, predicate: Label)]
            var po_pairs: [(predicate: Label, object: KBEntity)]
            var links: [Label]

            let subject = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "subject")
            let object = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "object")
            let predicate = "predicate"

            try subject.link(to: object, withPredicate: predicate)

            let entities = try KnowledgeStoreTestCase._sharedStore.entities()
            XCTAssert(entities.count == 2, "All entities are 2 (subject and object). \(entities.count)")
            XCTAssert(entities.contains(subject), "Retrieved subject")
            XCTAssert(entities.contains(object), "Retrieved object")

            let triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: nil)
            XCTAssert(triples.count == 1, "Insert first triple once results in 1 triple. \(triples.count)")
            XCTAssert(triples[0].subject == subject.identifier, "Insert first triple once results in 1 triple: subject")
            XCTAssert(triples[0].predicate == predicate, "Insert first triple once results in 1 triple: predicate")
            XCTAssert(triples[0].object == object.identifier, "Insert first triple once results in 1 triple: object")
            XCTAssert(triples[0].weight == 1, "weight is 1. \(triples[0].weight)")

            let tripleCondition = KBTripleCondition(subject: subject.identifier,
                                                    predicate: predicate,
                                                    object: object.identifier)
            let strictTriples = try KnowledgeStoreTestCase._sharedStore.triples(matching: tripleCondition)
            XCTAssert(triples.count == strictTriples.count, "String or nil condition return same result with one triple. \(triples.count), \(strictTriples.count)")
            XCTAssert(triples[0] == strictTriples[0], "String or nil condition return same result with one triple")
            XCTAssert(triples[0].weight == strictTriples[0].weight, "weight is 1. \(triples[0].weight)")

            try links = subject.links(to: object)
            XCTAssert(links.count == 1, "predicateLabelsTo: created link")
            XCTAssert(links[0] == predicate, "predicateLabelsTo: created link name")

            po_pairs = try subject.linkedEntities()
            XCTAssert(po_pairs.count == 1, "linkedEntities: created link")
            XCTAssert(po_pairs[0].0 == predicate, "linkedEntities: created link name")
            XCTAssert(po_pairs[0].1 == object, "linkedEntities: linked entity")

            po_pairs = try subject.linkedEntities(withPredicate: predicate)
            XCTAssert(po_pairs.count == 1, "linkedEntities (with label): 1 found")
            XCTAssert(po_pairs[0].1 == object, "linkedEntities: retrieve linked entity")

            po_pairs = try subject.linkedEntities(withPredicate: predicate, complement: true)
            XCTAssert(po_pairs.count == 0, "linkedEntities (with negated label): 0 found")

            po_pairs = try subject.linkedEntities(withPredicate: "NONMATCHING-\(predicate)")
            XCTAssert(po_pairs.count == 0, "linkedEntities (with other label): 0 found")

            po_pairs = try subject.linkedEntities(withPredicate: "NONMATCHING-\(predicate)", complement: true)
            XCTAssert(po_pairs.count == 1, "linkedEntities (with negated other label): some found")
            XCTAssert(po_pairs[0].1 == object, "linkedEntities: linked entity")

            sp_pairs = try subject.linkingEntities()
            XCTAssert(sp_pairs.count == 0, "linkingEntities to subject: 0 found")

            sp_pairs = try subject.linkingEntities(withPredicate: "ANYLABEL")
            XCTAssert(sp_pairs.count == 0, "linkingEntities to subject: 0 found")

            sp_pairs = try subject.linkingEntities(withPredicate: "ANYLABEL", complement: true)
            XCTAssert(sp_pairs.count == 0, "linkingEntities to subject (negated): 0 found")

            sp_pairs = try object.linkingEntities()
            XCTAssert(sp_pairs.count == 1, "linkingEntities to object: 1 found")
            XCTAssert(sp_pairs[0].0 == subject, "linkingEntities to object: linking entity")
            XCTAssert(sp_pairs[0].1 == predicate, "linkingEntities to object: created link name")

            sp_pairs = try object.linkingEntities(withPredicate: predicate)
            XCTAssert(sp_pairs.count == 1, "linkingEntities to object with predicate: 1 found")
            XCTAssert(sp_pairs[0].subject == subject, "linkingEntities to object with predicate: retrieved subject")

            sp_pairs = try object.linkingEntities(withPredicate: predicate, complement: true)
            XCTAssert(sp_pairs.count == 0, "linkingEntities to object with label (negated): 0 found")

            sp_pairs = try object.linkingEntities(withPredicate: "ANYLABEL")
            XCTAssert(sp_pairs.count == 0, "linkingEntities to object: 0 found")

            links = try subject.links(to: object)
            XCTAssert(links.count == 1, "links subject-object: 1 found")
            XCTAssert(links[0] == predicate, "links: predicate label matches")

            links = try subject.links(to: subject)
            XCTAssert(links.count == 0, "selflinks to subject: 0 found")

            links = try object.links(to: object)
            XCTAssert(links.count == 0, "selflinks to object: 0 found")
        } catch {
            XCTFail("\(error)")
        }
    }

    func testMultipleLinking() {
        do {
            var sp_pairs: [(subject: KBEntity, predicate: Label)]
            var po_pairs: [(predicate: Label, object: KBEntity)]
            var links: [Label]

            let subject = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "subject")
            let predicate = "predicate"
            let secondPredicate = "newPredicate"
            let object = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "object")
            let secondObject = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "secondObject")

            try subject.link(to: object, withPredicate: predicate)
            try subject.link(to: object, withPredicate: secondPredicate)
            try subject.link(to: secondObject, withPredicate: predicate)

            links = try subject.links(to: object)
            XCTAssert(links.count == 2, "\(links.count)")
            XCTAssert(links.contains(predicate), "created second link name")
            XCTAssert(links.contains(secondPredicate), "created link name")

            links = try subject.links(to: secondObject)
            XCTAssert(links.count == 1, "predicateLabelsTo: created second link")
            XCTAssert(links[0] == predicate, "predicateLabelsTo: created link name to second entity")

            po_pairs = try subject.linkedEntities()
            XCTAssert(po_pairs.count == 3, "linkedEntities: created link")
            XCTAssert(po_pairs.map({ $0.predicate }).contains(predicate), "linkedEntities: created link name")
            XCTAssert(po_pairs.map({ $0.object }).contains(object), "linkedEntities: linked entity")
            XCTAssert(po_pairs.map({ $0.predicate }).contains(secondPredicate), "linkedEntities: created second link name")
            XCTAssert(po_pairs.map({ $0.object }).contains(object), "linkedEntities: linked entity")
            XCTAssert(po_pairs.map({ $0.predicate }).contains(predicate), "linkedEntities: created link name to second entity")
            XCTAssert(po_pairs.map({ $0.object }).contains(secondObject), "linkedEntities: linked second entity")

            po_pairs = try subject.linkedEntities(withPredicate: predicate)
            XCTAssert(po_pairs.count == 2, "linkedEntities (with label): 1 found")
            XCTAssert(po_pairs.map({ $0.object }).contains(object), "linkedEntities: retrieve linked entity")
            XCTAssert(po_pairs.map({ $0.object }).contains(secondObject), "linkedEntities: retrieve second linked entity")

            po_pairs = try subject.linkedEntities(withPredicate: secondPredicate)
            XCTAssert(po_pairs.count == 1, "linkedEntities (with label): 1 found")
            XCTAssert(po_pairs[0].object == object, "linkedEntities: retrieve linked entity")

            po_pairs = try subject.linkedEntities(withPredicate: predicate, complement: true)
            XCTAssert(po_pairs.count == 1, "linkedEntities (with negated label): 1 expected \(po_pairs.count) found")
            XCTAssert(po_pairs[0].object == object, "linkedEntities (with negated label): second entity")

            po_pairs = try subject.linkedEntities(withPredicate: secondPredicate, complement: true)
            XCTAssert(po_pairs.count == 2, "linkedEntities (with negated label): 2 expected \(po_pairs.count) found")
            XCTAssert(po_pairs.map({ $0.object }).contains(object), "linkedEntities (with negated label): first entity")
            XCTAssert(po_pairs.map({ $0.object }).contains(secondObject), "linkedEntities (with negated label): second entity")

            links = try subject.links(to: object)
            XCTAssert(links.count == 2, "predicateLabelsTo: created link")
            XCTAssert(links.contains(predicate), "predicateLabelsTo: created link name")
            XCTAssert(links.contains(secondPredicate), "predicateLabelsTo: created second link name")

            links = try subject.links(to: secondObject)
            XCTAssert(links.count == 1, "predicateLabelsTo: created second link")
            XCTAssert(links[0] == predicate, "predicateLabelsTo: created link name to second entity")

            sp_pairs = try object.linkingEntities()
            XCTAssert(sp_pairs.count == 2, "linkingEntities from object: 2 found")
            XCTAssert(sp_pairs.map({ $0.subject }).contains(subject), "linkingEntities from object: subject-predicate")
            XCTAssert(sp_pairs.map({ $0.predicate }).contains(predicate), "linkingEntities from object: predicate")
            XCTAssert(sp_pairs.map({ $0.subject }).contains(subject), "linkingEntities from object: subject-secondPredicate")
            XCTAssert(sp_pairs.map({ $0.predicate }).contains(secondPredicate), "linkingEntities from object: secondPredicate")

            sp_pairs = try secondObject.linkingEntities()
            XCTAssert(sp_pairs.count == 1, "linkingEntities from secondObject: 1 found")
            XCTAssert(sp_pairs[0].0 == subject, "linkingEntities from secondObject: subject-predicate")
            XCTAssert(sp_pairs[0].1 == predicate, "linkingEntities from secondObject: predicate")

            sp_pairs = try object.linkingEntities(withPredicate: predicate)
            XCTAssert(sp_pairs.count == 1, "linkingEntities (with label): 1 found")
            XCTAssert(sp_pairs[0].subject == subject, "linkingEntities: retrieve linked entity")

            sp_pairs = try object.linkingEntities(withPredicate: secondPredicate)
            XCTAssert(sp_pairs.count == 1, "linkingEntities (with label): 1 found")
            XCTAssert(sp_pairs[0].subject == subject, "linkingEntities: retrieve linked entity")

            sp_pairs = try secondObject.linkingEntities(withPredicate: predicate)
            XCTAssert(sp_pairs.count == 1, "linkingEntities (with label): 1 found")
            XCTAssert(sp_pairs[0].subject == subject, "linkingEntities: retrieve linked entity")

            po_pairs = try subject.linkedEntities(withPredicate: secondPredicate)
            XCTAssert(po_pairs.count == 1, "linkedEntities (with label): 1 found")
            XCTAssert(po_pairs[0].object == object, "linkedEntities: retrieve linked entity")

            po_pairs = try subject.linkedEntities(withPredicate: predicate, complement: true)
            XCTAssert(po_pairs.count == 1, "linkedEntities (with negated label): 1 expected \(po_pairs.count) found")
            XCTAssert(po_pairs[0].object == object, "linkedEntities (with negated label): second entity")

            po_pairs = try subject.linkedEntities(withPredicate: secondPredicate, complement: true)
            XCTAssert(po_pairs.count == 2, "linkedEntities (with negated label): 2 expected \(po_pairs.count) found")
            XCTAssert(po_pairs.map({ $0.object }).contains(object), "linkedEntities (with negated label): first entity")
            XCTAssert(po_pairs.map({ $0.object }).contains(secondObject), "linkedEntities (with negated label): second entity")

            po_pairs = try subject.linkedEntities(withPredicate: "NONMATCHING-\(predicate)")
            XCTAssert(po_pairs.count == 0, "linkedEntities (with other label): 0 found")

            po_pairs = try subject.linkedEntities(withPredicate: "NONMATCHING-\(predicate)", complement: true)
            XCTAssert(po_pairs.count == 3, "linkedEntities (with negated other label): some found")

            links = try subject.links(to: object)
            XCTAssert(links.count == 2, "links: 2 found")
            XCTAssert(links.contains(predicate), "links: predicate label matches")
            XCTAssert(links.contains(secondPredicate), "links: second predicate label matches")

            links = try subject.links(to: secondObject)
            XCTAssert(links.count == 1, "secondObjectlinks: 1 found")
            XCTAssert(links[0] == predicate, "secondObjectlinks: predicate label matches")

            links = try subject.links(to: subject)
            XCTAssert(links.count == 0, "selflinks to subject: 0 found")

            links = try object.links(to: secondObject)
            XCTAssert(links.count == 0, "selflinks to object: 0 found")

            links = try object.links(to: object)
            XCTAssert(links.count == 0, "selflinks to object: 0 found")
        } catch {
            XCTFail("\(error)")
        }
    }

    func testJSONLDSerialization() {
        let expectation = XCTestExpectation(description: #function)

        do {
            let subject = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "subject")
            let predicate = "predicate"
            let secondPredicate = "newPredicate"
            let object = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "object")
            let secondObject = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "secondObject")

            let graph = KBJSONLDGraph(withEntities: [subject, object, secondObject])

            try subject.link(to: object, withPredicate: predicate)
            try subject.link(to: object, withPredicate: secondPredicate)
            try subject.link(to: secondObject, withPredicate: predicate)

            graph.linkedData() { result in
                switch result {
                case .failure(let err):
                    XCTFail("\(err)")
                case .success(let entities):
                    let expectedEntities: [KBKVPairs] = [["@id": "subject",
                                                             "predicate": ["object",
                                                                           "secondObject"],
                                                             "newPredicate": "object"],
                                                            ["@id": "object"],
                                                            ["@id": "secondObject"]]
                    XCTAssert(self.isEqual(entities, expectedEntities))

                    expectation.fulfill()
                }
            }
        } catch {
            XCTFail("Linking failed: \(error)")
        }

        wait(for: [expectation], timeout: 10.0)
    }

    func testJSONLDDeserialization() {
        let expectation = XCTestExpectation(description: #function)

        let bundle = Bundle(for: type(of: self).self)
        guard let path = bundle.path(forResource: "testJSONLDDeserialization", ofType: "json") else {
            XCTFail("Missing JSON file")
            expectation.fulfill()
            return
        }

        KnowledgeStoreTestCase._sharedStore.importContentsOf(JSONLDFileAt: path) { result in
            switch result {
            case .failure(let err):
                XCTFail("\(String(describing: err))")
                expectation.fulfill()
            case .success():
                do {
                    let entities = try KnowledgeStoreTestCase._sharedStore.entities()
                    let expectedCount: Int = 14
                    XCTAssert(entities.count == expectedCount, "There are \(expectedCount) entities in the store")

                    let timCook = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "http://www.timcook.com")
                    let jp = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "_:jp")
                    let gennaro = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "_:gennaro")

                    XCTAssert(entities.contains(timCook), "One of the entities is Tim Cook")
                    XCTAssert(entities.contains(jp), "One of the entities is Joao Pedro")
                    XCTAssert(entities.contains(gennaro), "One of the entities is Gennaro")

                    let tc_knows = try timCook.linkedEntities(withPredicate: "knows")
                    XCTAssert(tc_knows.count == 3, "Tim Cook knows 3 people")
                    XCTAssert(tc_knows.map({ $0.object }).contains(gennaro), "Tim Cook knows Gennaro")
                    XCTAssert(tc_knows.map({ $0.object }).contains(jp), "Tim Cook knows Joao Pedro")

                    let gennaro_knows = try gennaro.linkedEntities(withPredicate: "knows")
                    XCTAssert(gennaro_knows.count == 1, "Gennaro knows 1 person")
                    XCTAssert(gennaro_knows.map({ $0.object }).contains(jp), "Gennaro knows Joao Pedro")
                }  catch {
                    XCTFail("\(error)")
                }

                expectation.fulfill()
            }
        }

        wait(for: [expectation], timeout: 10.0)
    }

    func testSimpleTripleQuery() {
        do {
            var triples: [KBTriple], condition: KBTripleCondition

            let first = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "first")
            let second = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "second")
            let third = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "third")
            try first.link(to: second, withPredicate: "one")
            try second.link(to: third, withPredicate: "two")

            condition = KBTripleCondition(subject: nil, predicate: nil, object: second.identifier)
            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)

            XCTAssert(triples.count == 1, "One triple matching (?,?,\(second)). \(triples.count)")
            XCTAssert(triples[0].subject == first.identifier, "Subject of triple matching (?,?,\(second))")
            XCTAssert(triples[0].object == second.identifier, "Object of triple matching (?,?,\(second))")
            XCTAssert(triples[0].predicate == "one", "Predicate of triple matching (?,?,\(second))")
            XCTAssert(triples[0].weight == 1, "weight of triple matching (?,?,\(second))")

            let firstPrime = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "first-prime")
            try firstPrime.link(to: second, withPredicate: "one-prime")

            condition = KBTripleCondition(subject: nil, predicate: nil, object: second.identifier)
            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: condition)

            XCTAssert(triples.count == 2, "Two triples matching (?,?,\(second))")
            XCTAssert(triples[0].subject == firstPrime.identifier || triples[1].subject == firstPrime.identifier, "Subject of triple matching (?,?,\(second))")
            XCTAssert(triples[0].weight == 1, "weight of triple matching (?,?,\(second)). \(triples[0].weight)")
        } catch {
            XCTFail("\(error)")
        }
    }

    func testComplexTripleQuery() {
        do {
            var triples = [KBTriple]()

            let first = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "first")
            let second = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "second")
            let third = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "third")
            let fourth = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "fourth")
            let fifth = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "fifth")
            try first.link(to: second, withPredicate: "positive")
            try first.link(to: second, withPredicate: "negative")
            try second.link(to: third, withPredicate: "_OTHER_")
            try second.link(to: third, withPredicate: "positive")
            try third.link(to: fourth, withPredicate: "negative")
            try fourth.link(to: fifth, withPredicate: "_OTHER_")

            let positiveCondition = KBTripleCondition(subject: nil, predicate: "positive", object: nil)
            let negativeCondition = KBTripleCondition(subject: nil, predicate: "negative", object: nil)

            // DISJUNCTION (or)

            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: positiveCondition.or(negativeCondition))

            XCTAssert(triples.count == 4, "4 triples matching the condition. Found \(triples.count)")
            XCTAssert(triples.filter({ $0.predicate == "positive" || $0.predicate == "negative" }).count == 4, "All triples have the constrained predicate")

            let firstLinkedTo = triples.filter({ $0.subject == "first" }).map({ $0.object })
            XCTAssert(firstLinkedTo.count == 2, "First is linked twice to second (positive and negative)")
            XCTAssert(firstLinkedTo.contains(second.identifier), "First is linked to second")

            let secondLinkedTo = triples.filter({ $0.subject == "second" })
            XCTAssert(secondLinkedTo.count == 1, "Second is linked once to third")
            XCTAssert(secondLinkedTo[0] == KBTriple(subject: "second",
                                                    predicate: "positive",
                                                    object: "third",
                                                    weight: 0),
                      "Second is linked to third")

            let thirdLinkedTo = triples.filter({ $0.subject == "third" })
            XCTAssert(thirdLinkedTo.count == 1, "Third is linked once to fourth")
            XCTAssert(thirdLinkedTo[0] == KBTriple(subject: "third",
                                                   predicate: "negative",
                                                   object: "fourth",
                                                   weight: 0),
                      "Third is linked to fourth")

            // CONJUNCTION (and)

            /* TODO(gennaro)
             * Replace current "and" semantics with join-like semantics
             */

            print("No triples with condition \(positiveCondition.and(negativeCondition))")

            triples = try KnowledgeStoreTestCase._sharedStore.triples(matching: positiveCondition.and(negativeCondition))

            XCTAssert(triples.count == 0, "No triples with condition \(positiveCondition.and(negativeCondition))")
        } catch {
            XCTFail("\(error)")
        }
    }

    func testSimplePerformanceExample() {
        // This is an example of a performance test case.
        measure {
            do {
                let subject = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "subject")
                let object = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "object")
                let predicate = "predicate"

                try subject.link(to: object, withPredicate: predicate)

                let linked = try subject.linkedEntities()
                XCTAssert(linked.count == 1, "[sync perfs] linking was successful")

                let linkedCouplesCondition = try KBTripleCondition.havingPredicate(predicate)
                let so_couples = try KnowledgeStoreTestCase._sharedStore.triples(matching: linkedCouplesCondition)
                XCTAssert(so_couples.count == 1, "[sync perfs] couples retrieved")
                XCTAssert(so_couples[0].subject == subject.identifier, "[sync perfs] subject matches")
                XCTAssert(so_couples[0].object == object.identifier, "[sync perfs] object matches")
            } catch {
                XCTFail("\(error)")
            }
        }
    }

     func testSimpleLinkingPerformances() {
         measure {
             do {
                 // Simple Graph
                 let yummlyApp = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "_:com.yummly")
                 let yummlyActivity = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "_:com.yummly.activity.recipe.92205")
                 try yummlyActivity.link(to: yummlyApp, withPredicate: "application")
             } catch {
                 XCTFail("\(error)")
             }
         }
     }

    func testSimpleLinking() {
        let yummlyApp = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "_:com.yummly")
        let yummlyActivity = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "_:com.yummly.activity.recipe.92205")

        do {
            try yummlyActivity.link(to: yummlyApp, withPredicate: "application")

            let entities = try KnowledgeStoreTestCase._sharedStore.entities()
            let expectedCount: Int = 2
            XCTAssert(entities.count == expectedCount, "There are \(expectedCount) entities in the store, found \(entities.count)")

            let links = try yummlyActivity.links(to: yummlyApp)
            XCTAssert(links.count == 1, "The activity is linked to the app: count. Found \(links.count) links")
            XCTAssert(links.first == "application", "The activity is linked to the app")
        } catch {
            XCTFail("\(error)")
        }
    }

     func testJSONLDImportPerformances() {
         measure {
             self.importJSONLD(named: "bigGraph")
         }
     }

    func testJSONLDImport() {
        self.importJSONLD(named: "bigGraph")
        let yummlyApp = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "_:com.yummly")
        let yummlyActivity = KnowledgeStoreTestCase._sharedStore.entity(withIdentifier: "_:com.yummly.activity.recipe.92205")
        do {
            let entities = try KnowledgeStoreTestCase._sharedStore.entities()
            let expectedCount: Int = 84
            XCTAssert(entities.count == expectedCount, "There should be \(expectedCount) entities in // the store. Found \(entities.count)")
            let links = try yummlyActivity.links(to: yummlyApp)
            XCTAssert(links.count == 1, "The activity is linked to the app: 1 link. Found // \(links.count)")
            XCTAssert(links.first == "application", "The activity is linked to the app")
        } catch {
            XCTFail("\(error)")
        }
    }

//     func testSimpleKBLogic() {
//         let expectation = XCTestExpectation(description: #function)
//
//         let cat = KnowledgeStoreTestCase.sharedKnowledgeStore.entity(withIdentifier: "cat")
//         let mammal = KnowledgeStoreTestCase.sharedKnowledgeStore.entity(withIdentifier: "mammal")
//         let predicate = "is"
//
//         let tiger = KnowledgeStoreTestCase.sharedKnowledgeStore.entity(withIdentifier: "tiger")
//
//         do {
//             let logic = KBLogic.ifExistsLink(predicate, to: cat)
//             try KnowledgeStoreTestCase.sharedKnowledgeStore.inferLink(to: mammal, withPredicate: predicate, when: logic)
//         } catch {
//             print("Could not install inference link `if \(predicate) to \(mammal)`: \(error)")
//             XCTFail("\(error)")
//         }
//
//         do {
//             try cat.link(to: mammal, withPredicate: predicate)
//             try tiger.link(to: cat, withPredicate: predicate)
//
//             let tigerLinks = try tiger.linkedEntities()
//             XCTAssert(tigerLinks.count > 1, "2 links from \(tiger). Found \(tigerLinks.count)")
//             XCTAssert(tigerLinks.map({ $0.1 }).contains(cat), "Previous was kept \(cat)")
//             XCTAssert(tigerLinks.map({ $0.1 }).contains(mammal), "New one is inferred \(mammal)")
//         } catch {
//             XCTFail("\(error)")
//         }
//
//         expectation.fulfill()
//
//         wait(for: [expectation], timeout: 10.0)
//     }
//
//    func testDiscoverabilityKBLogic() {
//        let expectation = XCTestExpectation(description: #function)
//
//        do {
//            let intent = KnowledgeStoreTestCase.sharedKnowledgeStore.entity(withIdentifier: "multiShotSmsIntent");
//            let verb = KnowledgeStoreTestCase.sharedKnowledgeStore.entity(withIdentifier: "smsVerb")
//            let recipient = KnowledgeStoreTestCase.sharedKnowledgeStore.entity(withIdentifier: "smsRecipient")
//            let body = KnowledgeStoreTestCase.sharedKnowledgeStore.entity(withIdentifier: "smsBody")
//
//            let intentToDiscover = KnowledgeStoreTestCase.sharedKnowledgeStore.entity(withIdentifier: "oneShotSmsIntent")
//
//            let hasVerb = KBLogic.ifExistsLink("has", to: verb)
//            let noBody = KBLogic.ifNotExistsLink("has", to: body)
//            let noRecipient = KBLogic.ifNotExistsLink("has", to: recipient)
//
//            try KnowledgeStoreTestCase.sharedKnowledgeStore.inferLink(to: intentToDiscover, withPredicate: "discoverability", when: hasVerb.and(noBody))
//            try KnowledgeStoreTestCase.sharedKnowledgeStore.inferLink(to: intentToDiscover, withPredicate: "discoverability", when: hasVerb.and(noRecipient))
//
//            try intent.link(to: verb, withPredicate: "has")
//            try intent.link(to: recipient, withPredicate: "has")
//
//            let linked = KBTripleCondition(subject: nil, predicate: nil, object: intentToDiscover.identifier)
//            let linking = KBTripleCondition(subject: intentToDiscover.identifier, predicate: nil, object: nil)
//
//            let triples = try KnowledgeStoreTestCase.sharedKnowledgeStore.triples(matching: linked.or(linking))
//                .filter { $0.predicate == "discoverability" && !$0.subject.beginsWith(RULE_PREFIX) }
//            XCTAssert(triples.count == 1, "<oneShotSmsIntent> has been inferred. \(triples.count)")
//            if (triples.count > 0) {
//                XCTAssert(triples[0].subject == intent.identifier, "<oneShotSmsIntent> has been inferred (subject)")
//                XCTAssert(triples[0].object == intentToDiscover.identifier, "<oneShotSmsIntent> has been inferred (object)")
//            } else {
//                XCTFail()
//            }
//        } catch {
//            XCTFail("\(error)")
//        }
//
//        expectation.fulfill()
//
//        wait(for: [expectation], timeout: 10.0)
//    }

    private func importJSONLD(named name: String) {
        let bundle = Bundle(for: type(of: self).self)
        if let path = bundle.path(forResource: name, ofType: "json") {
            let dispatch = KBTimedDispatch(timeout: .distantFuture)

            KnowledgeStoreTestCase._sharedStore.importContentsOf(JSONLDFileAt: path) { result in
                switch result {
                case .failure(let err):
                    dispatch.interrupt(err)
                case .success():
                    dispatch.semaphore.signal()
                }
            }

            do {
                try dispatch.wait()
            } catch KnowledgeBase.KBError.timeout {
                XCTFail("Timeout importing JSON file")
            } catch {
                XCTFail("Failure importing JSON file: \(error)")
            }
        } else {
            XCTFail("Missing JSON file")
        }
    }
}
