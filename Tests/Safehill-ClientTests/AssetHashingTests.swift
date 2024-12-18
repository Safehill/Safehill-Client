import XCTest
@testable import Safehill_Client
import opencv2

final class Safehill_AssetHashingTests: XCTestCase {
    
    func testIdempotency() async throws {
        for _ in [0...10] {
            guard let data = Data(base64Encoded: SmallTestImageData) else {
                XCTFail()
                return
            }
            guard let data2 = Data(base64Encoded: SmallTestImageData) else {
                XCTFail()
                return
            }
            
            let hash1 = try SHHashingController.perceptualHash(for: data)
            let hash2 = try SHHashingController.perceptualHash(for: data2)
            
            XCTAssertEqual(hash1, hash2)
            
            let mat1 = try Mat.from(hash: hash1)
            let mat2 = try Mat.from(hash: hash2)
            
            XCTAssertEqual(hash1, mat1.hash())
            XCTAssertEqual(hash2, mat2.hash())
            
            let delta = try SHHashingController.compare(hash1, hash2)
            XCTAssertEqual(delta, 0)
        }
    }
    
    func testSameImageDiffRez() async throws {
        guard let data = Data(base64Encoded: SmallTestImageData) else {
            XCTFail()
            return
        }
        
        guard let largeData = Data(base64Encoded: LargeTestImageDataEncoded) else {
            XCTFail()
            return
        }
        
        let perceptualHash = try SHHashingController.perceptualHash(for: data)
        XCTAssertEqual(perceptualHash, "abfffaaefbebbefb")
        
        let largeImagePerceptualHash = try SHHashingController.perceptualHash(for: largeData)
        XCTAssertEqual(largeImagePerceptualHash, "af42fa946f6b8435")
        XCTAssertNotEqual(perceptualHash, largeImagePerceptualHash)
        
        let distance = try SHHashingController.compare(perceptualHash, largeImagePerceptualHash)
        
        XCTAssertEqual(distance, 24)
    }
}
