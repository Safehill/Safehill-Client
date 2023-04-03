//
//  RDFStorageGenericTest.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/9/21.
//

import XCTest
@testable import RDFStorage

class TestTripleStore: NSObject, TripleStore {
    
    var name: String = "TestTripleStore"
    
    func insertTriple(withSubject subject: String, predicate: String, object: String, completionHandler: @escaping (Error?) -> Void) {
        // TODO: Implement in-memory triple store
    }
    
}

class CRDFStorageGenericTest : XCTestCase {

    func testSPARQLQuery() {
        let storage = BaseRDFStore(tripleStore: TestTripleStore())
        do {
            try storage.execute(SPARQLQuery: "SELECT")
        } catch {
            XCTFail()
        }
    }
}

