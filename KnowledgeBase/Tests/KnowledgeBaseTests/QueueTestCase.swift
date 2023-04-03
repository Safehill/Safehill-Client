//
//  QueueTestCase.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/4/21.
//

import Foundation

@testable import KnowledgeBase
import XCTest

class KBQueueTestCase: KVStoreTestCase {
    
    private static let _sharedStore = KBQueueStore.store(.inMemory, type: .fifo)
    
    override func sharedStore() -> KBQueueStore {
        return KBQueueTestCase._sharedStore
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
    
    func testFifoQueue() throws {
        let items = ["Hello", "world"]
        let itemIds = ["first", "second"]
        try self.sharedStore().enqueue(items[0], withIdentifier: itemIds[0])
        try self.sharedStore().enqueue(items[1], withIdentifier: itemIds[1])
        var allItems = try self.sharedStore().peekItems(createdWithin: DateInterval.init(start: .distantPast, end: Date()), limit: 10)
        XCTAssertEqual(allItems.count, 2)
        
        var count = 0
        while let item = try self.sharedStore().dequeue() {
            XCTAssertEqual(item.identifier, itemIds[count])
            XCTAssertEqual(item.content as! String, items[count])
            count += 1
        }
        
        allItems = try self.sharedStore().peekItems(createdWithin: DateInterval.init(start: .distantPast, end: Date()), limit: 10)
        XCTAssertEqual(allItems.count, 0)
    }
    
    func testLifoQueue() throws {
        let store = KBQueueStore(.inMemory, type: .lifo)
        
        let items = ["Hello", "world"]
        let itemIds = ["first", "second"]
        try store.enqueue(items[0], withIdentifier: itemIds[0])
        try store.enqueue(items[1], withIdentifier: itemIds[1])
        var allItems = try store.peekItems(createdWithin: DateInterval.init(start: .distantPast, end: Date()), limit: 10)
        XCTAssertEqual(allItems.count, 2)
        
        var count = 1
        while let item = try store.dequeue() {
            XCTAssertEqual(item.identifier, itemIds[count])
            XCTAssertEqual(item.content as! String, items[count])
            count -= 1
        }
        
        allItems = try store.peekItems(createdWithin: DateInterval.init(start: .distantPast, end: Date()), limit: 10)
        XCTAssertEqual(allItems.count, 0)
    }
}
