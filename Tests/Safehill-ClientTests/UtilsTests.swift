import XCTest
@testable import Safehill_Client


class ArrayChunkingTests: XCTestCase {
    
    func testChunking() {
        let numbers = Array(1...10)
        let chunks = numbers.chunkedWithLinearDecrease()

        XCTAssertEqual(chunks.count, 3)
        XCTAssertEqual(chunks[0], [1, 2, 3, 4, 5])
        XCTAssertEqual(chunks[1], [6, 7, 8])
        XCTAssertEqual(chunks[2], [9, 10])
        XCTAssertEqual(numbers, Array(1...10))  // Ensure original array is not modified
    }
    
    func testEmptyArray() {
        let emptyArray: [Int] = []
        let chunks = emptyArray.chunked(into: 5)
        XCTAssertTrue(chunks.isEmpty)
    }

    func testInvalidChunkSize() {
        let numbers = Array(1...10)
        let chunks = numbers.chunked(into: 0)
        XCTAssertTrue(chunks.isEmpty)
    }

    func testScalability() {
        let largeArray = Array(1...100_000)
        let startTime = Date()
        let chunks = largeArray.chunked(into: 1000)
        let duration = Date().timeIntervalSince(startTime)
        XCTAssertLessThan(duration, 0.5, "Chunking a large array should be efficient")
        XCTAssertEqual(chunks.count, 100)
    }
    
    func testScalabilityWithLinearDecrease() {
        let largeArray = Array(1...100_000)
        let start = CFAbsoluteTimeGetCurrent()
        let _ = largeArray.chunkedWithLinearDecrease()
        let end = CFAbsoluteTimeGetCurrent()
        let duration = CFAbsoluteTime(end - start)
        XCTAssertLessThan(duration, 0.5, "Chunking a large array should be efficient")
    }

    func testPercentageBounds() {
        let numbers = Array(1...100)
        let chunks = numbers.chunkedWithLinearDecrease()

        var totalElements = 0
        for chunk in chunks {
            totalElements += chunk.count
        }
        XCTAssertEqual(totalElements, numbers.count)

        for (index, chunk) in chunks.enumerated() {
            let chunkSize = Double(chunk.count)
            let totalSize = Double(numbers.count)
            let percentage = chunkSize / totalSize * 100

            if index == 0 {
                XCTAssertGreaterThanOrEqual(percentage, 30.0)
                XCTAssertLessThanOrEqual(percentage, 50.0)
            } else {
                XCTAssertLessThan(percentage, 40.0)
            }
        }
        XCTAssertEqual(numbers, Array(1...100))  // Ensure original array is not modified
    }
}
