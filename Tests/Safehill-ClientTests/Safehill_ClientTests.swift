import XCTest
@testable import Safehill_Client

final class Safehill_ClientTests: XCTestCase {
    func testSubtract() throws {
        let first = ["Alice", "Bob", "Cindy"]
        let second = ["Bob", "Mary"]
        
        XCTAssert(first.subtract(second).elementsEqual(["Alice", "Cindy"]))
    }
}
