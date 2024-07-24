import XCTest
@testable import Safehill_Client
import os

struct DummyRestorationDelegate: SHAssetActivityRestorationDelegate {
    func restoreUploadHistoryItems(from: [String : [(SHUploadHistoryItem, Date)]]) {}
    
    func restoreShareHistoryItems(from: [String : [(SHShareHistoryItem, Date)]]) {}
}

class DummyOperation: Operation, SHBackgroundOperationProtocol {
    typealias OperationResult = Result<Void, Error>
    
    var log = Logger(subsystem: "com.gf.safehill.tests", category: "DummyOperation")
    
    func run(qos: DispatchQoS.QoSClass, completionHandler: @escaping (Result<Void, Error>) -> Void) {
        DispatchQueue.global(qos: qos).async {
            let dateFormatter = DateFormatter()
            dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
            
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) running operation \(String(describing: Self.self), privacy: .public)")
            
            completionHandler(.success(()))
        }
    }
}

final class Safehill_BackgroundOperationTests: XCTestCase {
    
    let log = Logger(subsystem: "com.gf.safehill.tests", category: "BackgroundOperation")
    
    func testBasicInvokeBackgroundOperation() async throws {
        
        let expectation = XCTestExpectation()
        
        let operation = DummyOperation()
        
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        
        let processor = SHBackgroundOperationProcessor<DummyOperation>()
        
        var timestamp = dateFormatter.string(from: Date())
        self.log.debug("\(timestamp, privacy: .public) run ONETIME operation \(String(describing: operation.self), privacy: .public)")
        
        await processor.runOperation(operation, qos: .default) { result in
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) completed ONETIME operation \(String(describing: operation.self), privacy: .public)")
        }
        
        var count = 1
        
        timestamp = dateFormatter.string(from: Date())
        self.log.debug("\(timestamp, privacy: .public) scheduling REPEAT operation \(String(describing: operation.self), privacy: .public) with delay=1 interval=2")
        
        await processor.runRepeatedOperation(operation, initialDelay: 1, repeatInterval: 2, qos: .default) { result in
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) completed scheduled REPEAT \(count) operation \(String(describing: operation.self), privacy: .public)")
            count += 1
        }
        
        timestamp = dateFormatter.string(from: Date())
        self.log.debug("\(timestamp, privacy: .public) run SECOND ONETIME operation \(String(describing: operation.self), privacy: .public)")
        
        await processor.runOperation(operation, qos: .default) { result in
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) completed SECOND ONETIME operation \(String(describing: operation.self), privacy: .public)")
        }
        
        try await Task.sleep(nanoseconds: 2 * 1_000_000_000)
        
        timestamp = dateFormatter.string(from: Date())
        self.log.debug("\(timestamp, privacy: .public) run THIRD ONETIME operation \(String(describing: operation.self), privacy: .public)")
        
        await processor.runOperation(operation, qos: .default) { result in
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) completed THIRD ONETIME operation \(String(describing: operation.self), privacy: .public)")
        }
        
        try await Task.sleep(nanoseconds: 1 * 1_000_000_000)
        
        timestamp = dateFormatter.string(from: Date())
        self.log.debug("\(timestamp, privacy: .public) stopping scheduled REPEAT operations \(String(describing: operation.self), privacy: .public)")
        
        await processor.stopRepeatedOperation()
        
        try await Task.sleep(nanoseconds: 5 * 1_000_000_000)
        
        timestamp = dateFormatter.string(from: Date())
        self.log.debug("\(timestamp, privacy: .public) fulfilling expectations")
        
        expectation.fulfill()
        
        await fulfillment(of: [expectation], timeout: 10)
    }
    
}
