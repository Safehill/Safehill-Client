import XCTest
@testable import KnowledgeBase

final class KBSPARQLTests: XCTestCase {
    func testSPARQL() {
        let insertExpectation = XCTestExpectation(description: "insert")
        
        let knowledgeStore = KBKnowledgeStore.defaultStore()
        knowledgeStore.importContentsOf(turtleFileAt: "") { result in
            switch result {
            case .failure(let err):
                XCTFail("\(err)")
                fallthrough
            default:
                insertExpectation.fulfill()
            }
        }
        
        let selectExpectation = XCTestExpectation(description: "select")
        
        knowledgeStore.execute(SPARQLQuery: "SELECT") { result in
            switch result {
            case .failure(let err):
                XCTFail("\(err)")
                selectExpectation.fulfill()
            case .success(let results):
                XCTAssert(results.isEmpty)
                selectExpectation.fulfill()
            }
        }
        
        wait(for: [insertExpectation, selectExpectation], timeout: 10.0)
    }
}
