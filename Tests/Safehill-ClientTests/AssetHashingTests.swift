import XCTest
@testable import Safehill_Client

final class Safehill_AssetHashingTests: XCTestCase {
    
    func testPerceptualHashing() async throws {
        guard let data = Data(base64Encoded: SmallTestImageData) else {
            XCTFail()
            return
        }
        let perceptualHash = try SHHashingController.perceptualHash(
            forImageData: data
        )
        XCTAssertEqual(perceptualHash, "")
    }
}
