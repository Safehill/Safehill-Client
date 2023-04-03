//
//  UserDefaultsKVStoreTestCase.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/11/21.
//

import XCTest
@testable import KnowledgeBase

class KBUserDefaultsKVStoreTests: KVStoreTestCase {

    private static let _sharedStore = KBKVStore.store(.userDefaults)
    
    override func testWriteBatch() throws {
        try super.testWriteBatch()
    }

    func testUserDefaultsRestored() {
        let userDefaultsStore = KBKnowledgeStore.store(.userDefaults)
        
        let dictionary = ["key": "value"]
        let key = "KeyToPersist"
        do {
            try KBUserDefaultsKVStoreTests._sharedStore.set(value: dictionary, for: key)
            let value = try userDefaultsStore.value(for: key)
            XCTAssertNotNil(value)
            XCTAssertNotNil(value as? NSDictionary)

            let userDefaultsStoreCopy = value as! NSDictionary
            XCTAssertEqual(userDefaultsStoreCopy.count, dictionary.count)
            XCTAssertNotNil(userDefaultsStoreCopy["key"] as? String)
            XCTAssertEqual(userDefaultsStoreCopy["key"] as? String, dictionary["key"])
            
            try KBUserDefaultsKVStoreTests._sharedStore.removeValue(for: key)
            let otherUserDefaultsHandler = KBKnowledgeStore.store(.userDefaults)
            XCTAssertNotNil(otherUserDefaultsHandler)
            let otherValue = try otherUserDefaultsHandler.value(for: key)
            XCTAssertNil(otherValue)
        } catch {
            XCTFail()
        }
    }

    func testPerformances() {
        measure {
            do {
                try self.testWriteBatch()
            } catch {
                XCTFail("\(error)")
            }
        }
    }
}
