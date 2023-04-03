//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/14/21.
//

import XCTest
@testable import KnowledgeBase

class KBHexaStoreTests: XCTestCase {
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testHexaTest() {
        XCTAssert("".isHexaKey() == false)
        XCTAssert("blah".isHexaKey() == false)
        XCTAssert("::blah::".isHexaKey() == false)
        XCTAssert("spo::blah::".isHexaKey() == false)
        XCTAssert("pso::blah::".isHexaKey() == false)
        XCTAssert("pso::blah::blah".isHexaKey() == false)
        
        XCTAssert("pso::blah::blah::blah".isHexaKey() == true)
        XCTAssert("spo::blah::blah::blah".isHexaKey() == true)
        
        XCTAssert("spo::blah::blah::blah::".isHexaKey() == false)
    }
    
}
