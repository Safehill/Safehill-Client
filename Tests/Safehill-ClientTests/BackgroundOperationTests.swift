import XCTest
@testable import Safehill_Client
import os

struct DummyRestorationDelegate: SHAssetActivityRestorationDelegate {
    func didStartRestoration() {}
    
    func restoreUploadQueueItems(forLocalIdentifiers: [String], in groupId: String) {}
    
    func restoreShareQueueItems(forLocalIdentifiers: [String], in groupId: String) {}
    
    func didCompleteRestoration(userIdsInvolvedInRestoration: [String]) {}
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
    
    func testBasicInvokeBackgroundOperation() throws {
        
        let expectation = XCTestExpectation()
        
        let operation = DummyOperation()
        
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        
        let processor = SHBackgroundOperationProcessor<DummyOperation>()
        
        let timestamp = dateFormatter.string(from: Date())
        self.log.debug("\(timestamp, privacy: .public) run one-time operation \(String(describing: operation.self), privacy: .public)")
        
        processor.runOperation(operation, qos: .default) { result in
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) completed one-time operation \(String(describing: operation.self), privacy: .public)")
        }
        
        var count = 1
        
        self.log.debug("\(timestamp, privacy: .public) scheduling operation \(String(describing: operation.self), privacy: .public) with delay=1 interval=2")
        
        processor.runRepeatedOperation(operation, initialDelay: 1, repeatInterval: 2, qos: .default) { result in
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) completed scheduled \(count) operation \(String(describing: operation.self), privacy: .public)")
            
            if count == 3 {
                self.log.debug("\(timestamp, privacy: .public) stopping scheduled operations \(String(describing: operation.self), privacy: .public)")
                processor.stopRepeatedOperation()
                expectation.fulfill()
            }
            count += 1
        }
        
        let timestamp2 = dateFormatter.string(from: Date())
        self.log.debug("\(timestamp2, privacy: .public) run SECOND one-time operation \(String(describing: operation.self), privacy: .public)")
        
        processor.runOperation(operation, qos: .default) { result in
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) completed SECOND one-time operation \(String(describing: operation.self), privacy: .public)")
        }
        
        DispatchQueue.global().asyncAfter(deadline: .now() + 2) {
            let timestamp = dateFormatter.string(from: Date())
            self.log.debug("\(timestamp, privacy: .public) run THIRD one-time operation \(String(describing: operation.self), privacy: .public)")
            
            processor.runOperation(operation, qos: .default) { result in
                let timestamp = dateFormatter.string(from: Date())
                self.log.debug("\(timestamp, privacy: .public) completed THIRD one-time operation \(String(describing: operation.self), privacy: .public)")
            }
        }
        
        wait(for: [expectation], timeout: 10)
    }
    
}
