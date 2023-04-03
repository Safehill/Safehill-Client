//
//  TripleTests.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/11/21.
//


@testable import KnowledgeBase
import XCTest

class KBTripleTests: XCTestCase {
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testEqualsHashValue() {
        // Create a set with KBTriple objects and assert same are not duplicated
        var tripleSet = Set<KBTriple>()
        
        let triple1 = KBTriple(subject: "s",
                               predicate: "p",
                               object: "o",
                               weight: 0)
        let triple2 = KBTriple(subject: "s",
                               predicate: "p",
                               object: "o2",
                               weight: 0)
        let triple3 = KBTriple(subject: "s2",
                               predicate: "p",
                               object: "o2",
                               weight: 0)
        
        tripleSet.insert(triple1)
        XCTAssert(tripleSet.count == 1, "\(tripleSet.count)")
        tripleSet.insert(triple1)
        tripleSet.insert(triple1)
        XCTAssert(tripleSet.count == 1, "\(tripleSet.count)")
        
        tripleSet.insert(triple2)
        tripleSet.insert(triple2)
        tripleSet.insert(triple2)
        tripleSet.insert(triple2)
        XCTAssert(tripleSet.count == 2, "\(tripleSet.count)")
        
        tripleSet.insert(triple3)
        XCTAssert(tripleSet.count == 3, "\(tripleSet.count)")
        
        // Make sure that weight doesn't influence equality
        
        let triple4 = KBTriple(subject: "s",
                               predicate: "p",
                               object: "o",
                               weight: 3)
        tripleSet.insert(triple4)
        XCTAssert(tripleSet.count == 3, "\(tripleSet.count)")
        
    }

    func testEncoding() {
        var triple: KBTriple, data: Data, unarchiver: NSKeyedUnarchiver, unarchived: Any?

        do {
            triple = KBTriple(subject: "s",
                              predicate: "p",
                              object: "o",
                              weight: 0)
            data = try NSKeyedArchiver.archivedData(withRootObject: triple, requiringSecureCoding: true)
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
            unarchived = unarchiver.decodeObject(of: KBTriple.self, forKey: NSKeyedArchiveRootObjectKey)
            XCTAssertNotNil(unarchived as? KBTriple)
            XCTAssert((unarchived as? KBTriple) == triple)
            XCTAssert((unarchived as! KBTriple).weight == triple.weight)
        } catch {
            XCTFail("error: \(error)")
        }
        
        do {
            triple = KBTriple(subject: "s",
                              predicate: "p",
                              object: "o",
                              weight: 99)
            data = try NSKeyedArchiver.archivedData(withRootObject: triple, requiringSecureCoding: true)
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
            unarchived = unarchiver.decodeObject(of: KBTriple.self, forKey: NSKeyedArchiveRootObjectKey)
            XCTAssertNotNil(unarchived as? KBTriple)
            XCTAssert((unarchived as? KBTriple) == triple)
            XCTAssert((unarchived as! KBTriple).weight == triple.weight)
        } catch {
            XCTFail("error: \(error)")
        }
        
    }
}

