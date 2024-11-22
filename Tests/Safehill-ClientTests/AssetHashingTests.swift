import XCTest
@testable import Safehill_Client

final class Safehill_AssetHashingTests: XCTestCase {
    
    func testIdempotency() async throws {
        guard let data = Data(base64Encoded: SmallTestImageData) else {
            XCTFail()
            return
        }
        let perceptualHash = try SHHashingController.perceptualHash(
            forImageData: data
        )
        XCTAssertEqual(perceptualHash, "-378606956254134784")
        
        let sameHash = try SHHashingController.perceptualHash(
            forImageData: data
        )
        XCTAssertEqual(perceptualHash, sameHash)
        
        guard let largeData = Data(base64Encoded: LargeTestImageDataEncoded) else {
            XCTFail()
            return
        }
        let largeImagePerceptualHash = try SHHashingController.perceptualHash(
            forImageData: largeData
        )
        XCTAssertNotEqual(perceptualHash, largeImagePerceptualHash)
        
        let distance = try SHHashingController.calculateDistanceBetween(
            lhsPerceptualHash: perceptualHash,
            rhsPerceptualHash: largeImagePerceptualHash
        )
        
        XCTAssertEqual(distance, 22)
    }
}
