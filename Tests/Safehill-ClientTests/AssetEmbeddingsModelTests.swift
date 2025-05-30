import XCTest
@testable import Safehill_Client


final class Safehill_AssetEmbeddingsModelTests: XCTestCase {
    
    func testModelLoading() async throws {
        let controller = SHAssetEmbeddingsController.shared
        try await controller.loadModelIfNeeded()
    }
}
