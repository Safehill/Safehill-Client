import Foundation
import os
import KnowledgeBase

public protocol SHBackgroundOperationProtocol : Operation {
    var log: Logger { get }
    
    /// Used when the same operation is recursed on the operation queue (see OperationQueueProcessor::repeat)
    /// - Returns: a new object initialized exactly as Self was
    func clone() -> SHBackgroundOperationProtocol
}

public protocol SHBackgroundQueueProcessorOperationProtocol : SHBackgroundOperationProtocol {
    func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem
}

open class SHAbstractBackgroundOperation : Operation {
    
    public enum State: String {
        case ready = "Ready"
        case executing = "Executing"
        case finished = "Finished"
        fileprivate var keyPath: String { return "is" + self.rawValue }
    }
    
    private let stateQueue = DispatchQueue(label: "com.gf.safehill.BackgroundOperation.stateQueue",
                                           attributes: .concurrent)
    private var _state: State = .ready
    
    public var state: State {
        get {
            stateQueue.sync {
                return _state
            }
        }
        set {
            let oldValue = state
            willChangeValue(forKey: state.keyPath)
            willChangeValue(forKey: newValue.keyPath)
            stateQueue.sync(flags: .barrier) {
                _state = newValue
            }
            didChangeValue(forKey: state.keyPath)
            didChangeValue(forKey: oldValue.keyPath)
        }
    }
    
    public override var isAsynchronous: Bool {
        return true
    }
    
    public func run(completionHandler: @escaping (Result<Void, Error>) -> Void) {
        fatalError("run(completionHandler:) should be overridden in a SHAbstractBackgroundOperation subclass")
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.run { _ in
            self.state = .finished
        }
    }
    
    public override func start() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .ready
        main()
    }
    
    public override var isExecuting: Bool {
        return state == .executing
    }
    
    public override var isFinished: Bool {
        return state == .finished
    }
}


open class SHBackgroundOperationProcessor<T: SHBackgroundOperationProtocol> {
    
    private let dispatchIntervalInSeconds: Int?
    private let delayedStartInSeconds: Int
    
    private var started = false
    private let stateQueue = DispatchQueue(label: "com.sh.AssetsUploadQueueProcessor.stateQueue",
                                           attributes: .concurrent)
    private let timerQueue = DispatchQueue(label: "com.sh.AssetsUploadQueueProcessor.timerQueue",
                                           attributes: .concurrent)
    private var timer: Timer? = nil
    
    let operationQueue = OperationQueue()
    
    public init(delayedStartInSeconds: Int = 0,
                dispatchIntervalInSeconds: Int? = nil) {
        guard delayedStartInSeconds >= 0 else {
            fatalError("can't start in the past")
        }
        guard dispatchIntervalInSeconds ?? 0 >= 0 else {
            fatalError("interval between operations in seconds needs to be a positive integer")
        }
        self.delayedStartInSeconds = delayedStartInSeconds
        self.dispatchIntervalInSeconds = dispatchIntervalInSeconds
    }
    
    public func `repeat`(_ operation: T) {
        guard self.started == false else { return }
        
        self.stateQueue.sync(flags: .barrier) {
            self.started = true
        }
        
        self.process(operation, after: self.delayedStartInSeconds)
    }
    
    private func process(_ operation: T, after delay: Int) {
        Dispatch.dispatchPrecondition(condition: .notOnQueue(DispatchQueue.main))
        
        guard self.started else { return }
        
        /// 
        /// As long as there is no operation in the queue that is running, add an operation by cloning the last one run.
        /// That operation will pick up any item in the queue, if any exists.
        /// If the queue is empty, then the upload operation will finish immediately
        ///
        
        self.timerQueue.sync {
            ///
            /// Timers need to be scheduled on the main queue
            ///
            DispatchQueue.main.async {
                var lastExecutingOperation: T = operation
                
                let interval: TimeInterval
                let repeating: Bool
                if let repeatInterval = self.dispatchIntervalInSeconds {
                    interval = Double(max(repeatInterval, delay))
                    repeating = true
                } else {
                    interval = Double(delay)
                    repeating = false
                }
                
                self.timer = Timer.scheduledTimer(withTimeInterval: interval, repeats: repeating) {
                    [weak self] _ in
                    
                    if !lastExecutingOperation.isExecuting,
                       let sself = self,
                       sself.started,
                       sself.operationQueue.operationCount == 0
                    {
                        lastExecutingOperation = lastExecutingOperation.clone() as! T
                        sself.operationQueue.addOperation(lastExecutingOperation)
                    }
                }
            }
        }
    }
    
    public func stopRepeat() {
        self.stateQueue.sync(flags: .barrier) {
            self.started = false
            self.operationQueue.cancelAllOperations()
        }
        self.timerQueue.sync {
            self.timer?.invalidate()
        }
    }
}
