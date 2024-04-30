import Foundation

public class SHBackgroundOperationProcessor<T: SHBackgroundOperationProtocol> {
    
    private var runningOperations = [String: Bool]()
    private var timers = [String: DispatchSourceTimer]()
    
    private let operationQueue: DispatchQueue
    
    var operationKey: String {
        String(describing: T.self)
    }
    
    internal init() {
        self.operationQueue = DispatchQueue(
            label: "com.gf.safehill.SHBackgroundOperationProcessor.\(String(describing: T.self))",
            attributes: .concurrent
        )
    }
    
    /// Run a single operation
    /// - Parameters:
    ///   - completion: the callback
    public func runOperation(
        _ operation: T,
        qos: DispatchQoS.QoSClass,
        completion: @escaping (T.OperationResult) -> Void
    ) {
        let operationKey = self.operationKey
        log.debug("\(operationKey): run at qos=\(qos.toTaskPriority().rawValue)")
        
        operationQueue.async { [weak self] in
            guard let self = self else { return }
            
            ///
            /// If another operation of the same type is running, skip this cycle
            ///
            guard self.runningOperations[operationKey] == nil || !self.runningOperations[operationKey]! else {
                return
            }
            
            self.runningOperations[operationKey] = true
        }
        
        operation.run(qos: qos) { [weak self] result in
            completion(result)
            
            self?.operationQueue.async {
                self?.runningOperations[operationKey] = false
            }
        }
    }
    
    /// Run an operation repeatedly with a given interval (and a delay)
    /// - Parameters:
    ///   - initialDelay: the initial delay
    ///   - repeatInterval: the interval between each run
    ///   - completion: the calback
    public func runRepeatedOperation(
        _ operation: T,
        initialDelay: TimeInterval,
        repeatInterval: TimeInterval,
        qos: DispatchQoS.QoSClass,
        completion: @escaping (T.OperationResult) -> Void
    ) {
        let operationKey = self.operationKey
        log.debug("\(operationKey): scheduled repeated run at qos=\(qos.toTaskPriority().rawValue) with delay=\(initialDelay) interval=\(repeatInterval)")
        
        let timer = DispatchSource.makeTimerSource(queue: operationQueue)
        timer.schedule(deadline: .now() + initialDelay, repeating: repeatInterval)
        timer.setEventHandler { [weak self] in
            self?.runOperation(operation, qos: qos, completion: completion)
        }
        timer.resume()
        
        operationQueue.async {
            self.timers[operationKey] = timer
        }
    }
    
    /// Stop an operation that was previously run on repeat
    /// - Parameter operationType: the operation type
    public func stopRepeatedOperation() {
        let operationKey = self.operationKey
        operationQueue.async {
            if let timer = self.timers[operationKey] {
                timer.cancel()
                self.timers[operationKey] = nil
            }
        }
    }
}


