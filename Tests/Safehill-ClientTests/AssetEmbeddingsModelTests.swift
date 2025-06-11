import XCTest
@testable import Safehill_Client


final class Safehill_AssetEmbeddingsModelTests: XCTestCase {
    
    func _testModelLoading() async throws {
        let controller = SHAssetEmbeddingsController.shared
        try await controller.loadModelIfNeeded()
    }
}
