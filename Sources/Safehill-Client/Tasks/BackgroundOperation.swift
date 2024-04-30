import Foundation
import os
import KnowledgeBase

public protocol SHBackgroundOperationProtocol : Operation {
    
    associatedtype OperationResult
    
    var log: Logger { get }
    
    /// Used when the same operation is recursed on the operation queue (see OperationQueueProcessor::repeat)
    /// - Returns: a new object initialized exactly as Self was
    func clone() -> any SHBackgroundOperationProtocol
    
    func run(completionHandler: @escaping (OperationResult) -> Void)
}

public protocol SHBackgroundQueueBackedOperationProtocol : SHBackgroundOperationProtocol {
    
    func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem
    
    func process(_: KBQueueItem, completionHandler: @escaping (Result<Void, Error>) -> Void)
}

open class SHBackgroundOperationProcessor {
    
    static let shared = SHBackgroundOperationProcessor()
    
    private var runningOperations = [String: Bool]()
    private var timers = [String: DispatchSourceTimer]()
    private let operationQueue = DispatchQueue(
        label: "com.sh.AssetsUploadQueueProcessor.operation",
        attributes: .concurrent
    )
    
    private init() {}
    
    /// Run a single operation
    /// - Parameters:
    ///   - operation: the operation
    ///   - completion: the callback
    func runOperation<T: SHBackgroundOperationProtocol>(
        _ operation: T,
        completion: @escaping (T.OperationResult) -> Void
    ) {
        let operationKey = String(describing: T.self)
        
        operationQueue.async(flags: .barrier) { [weak self] in
            guard let self = self else { return }
            
            ///
            /// If another operation of the same type is running, skip this cycle
            ///
            guard self.runningOperations[operationKey] == nil || !self.runningOperations[operationKey]! else {
                return
            }
            
            self.runningOperations[operationKey] = true
        }
        
        operation.run { [weak self] result in
            completion(result)
            
            self?.operationQueue.async(flags: .barrier) {
                self?.runningOperations[operationKey] = false
            }
        }
    }
    
    /// Run an operation repeatedly with a given interval
    /// - Parameters:
    ///   - operation: the operation
    ///   - initialDelay: the initial delay
    ///   - repeatInterval: the interval between each run
    ///   - completion: the calback
    func runRepeatedOperation<T: SHBackgroundOperationProtocol>(
        _ operation: T, initialDelay: TimeInterval,
        repeatInterval: TimeInterval,
        completion: @escaping (T.OperationResult) -> Void
    ) {
        let operationKey = String(describing: T.self)
        
        let timer = DispatchSource.makeTimerSource(queue: operationQueue)
        timer.schedule(deadline: .now() + initialDelay, repeating: repeatInterval)
        timer.setEventHandler { [weak self] in
            self?.runOperation(operation, completion: completion)
        }
        timer.resume()
    }
    
    /// Stop an operation that was previously run on repeat
    /// - Parameter operationType: the operation type
    func stopRepeatedOperation<T: SHBackgroundOperationProtocol>(
        _ operationType: T.Type
    ) {
        let operationKey = String(describing: T.self)
        
        operationQueue.async(flags: .barrier) {
            if let timer = self.timers[operationKey] {
                timer.cancel()
                self.timers[operationKey] = nil
            }
        }
    }
}

