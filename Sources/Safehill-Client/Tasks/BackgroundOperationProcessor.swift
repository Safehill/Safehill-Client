import Foundation

public actor SHBackgroundOperationProcessor<T: SHBackgroundOperationProtocol> {
    
    private var runningOperations = [String: Bool]()
    private var tasks = [String: Task<(), any Error>]()
    
    var operationKey: String {
        String(describing: T.self)
    }
    
    /// Check if an operation is running
    private func isOperationRunning(_ key: String) -> Bool {
        return runningOperations[key] ?? false
    }

    /// Set the running state of an operation
    private func setOperationRunning(_ key: String, running: Bool) {
        runningOperations[key] = running
    }
    
    /// Run a single operation
    /// - Parameters:
    ///   - completion: the callback
    public func runOperation(
        _ operation: T,
        qos: DispatchQoS.QoSClass,
        completion: @escaping (T.OperationResult) -> Void
    ) {
        Task { [weak self] in
            guard let self = self else { return }
            let operationKey = await self.operationKey
            log.debug("\(operationKey): run at qos=\(qos.toTaskPriority().rawValue)")
            
            ///
            /// If another operation of the same type is running, skip this cycle
            ///
            let isRunning = await self.runningOperations[operationKey] ?? false
            guard !isRunning else {
                return
            }
            
            await self.setOperationRunning(operationKey, running: true)
            operation.run(qos: qos) { result in
                completion(result)
                Task { [weak self] in
                    guard let self = self else { return }
                    await self.setOperationRunning(operationKey, running: false)
                }
            }
        }
    }
    
    /// Run an operation repeatedly with a given interval (and a delay)
    /// - Parameters:
    ///   - initialDelay: the initial delay
    ///   - repeatInterval: the interval between each run
    ///   - completion: the callback
    public func runRepeatedOperation(
        _ operation: T,
        initialDelay: TimeInterval,
        repeatInterval: TimeInterval,
        qos: DispatchQoS.QoSClass,
        completion: @escaping (T.OperationResult) -> Void
    ) {
        let operationKey = self.operationKey
        log.debug("\(operationKey, privacy: .public): scheduled repeated run at qos=\(qos.toTaskPriority().rawValue) with delay=\(initialDelay) interval=\(repeatInterval)")
        
        let task = Task { [weak self] in
            guard let self = self else { return }
            try await Task.sleep(nanoseconds: UInt64(initialDelay * 1_000_000_000))
            
            while !Task.isCancelled {
                await self.runOperation(operation, qos: qos, completion: completion)
                try await Task.sleep(nanoseconds: UInt64(repeatInterval * 1_000_000_000))
            }
        }
        
        tasks[operationKey] = task
    }
    
    /// Stop an operation that was previously run on repeat
    public func stopRepeatedOperation() {
        let operationKey = self.operationKey
        if let task = tasks[operationKey] {
            task.cancel()
            tasks[operationKey] = nil
        }
    }
}
