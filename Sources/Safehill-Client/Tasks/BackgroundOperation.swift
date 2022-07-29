//
//  BackgroundOperation.swift
//  Safehill-Client
//
//  Created by Gennaro Frazzingaro on 9/12/21.
//

import Foundation
import os
import KnowledgeBase

public protocol SHBackgroundOperationProtocol : Operation {
    var log: Logger { get }
    
    /// Used when the same operation is recursed on the operation queue (see OperationQueueProcessor::repeat)
    /// - Returns: a new object initialized exactly as Self was
    func clone() -> SHBackgroundOperationProtocol
    
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
                                           qos: .background,
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


open class SHOperationQueueProcessor<T: SHBackgroundOperationProtocol> {
    
    private let dispatchIntervalInSeconds: Int?
    private let delayedStartInSeconds: Int
    
    private var started = false
    private let stateQueue = DispatchQueue(label: "com.sh.AssetsUploadQueueProcessor.stateQueue",
                                           qos: .background,
                                           attributes: .concurrent)
    private let timerQueue = DispatchQueue(label: "com.sh.AssetsUploadQueueProcessor.timerQueue",
                                           qos: .background,
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
        
        self.stateQueue.sync {
            self.started = true
        }
        // If there is no operation in the queue, continously add an operation.
        // That operation will pick up any item in the queue, if any exists.
        // If the queue is empty, then the upload operation will finish immediately
        self.timerQueue.sync { [weak self] in
            self?.process(operation, after: self!.delayedStartInSeconds)
        }
    }
    
    private func process(_ operation: T, after seconds: Int) {
        if self.started && operationQueue.operationCount == 0 {
            self.timerQueue.sync {
                self.timer = Timer.scheduledTimer(withTimeInterval: Double(seconds), repeats: false, block: { [weak self] _ in
                    if !operation.isExecuting && self?.operationQueue.operationCount == 0 {
                        self?.operationQueue.addOperation(operation.clone() as! T)
                    }
                })
            }
        }
        
        if let dispatchIntervalInSeconds = self.dispatchIntervalInSeconds {
            self.timerQueue.sync {
                let dispatchInterval = max(dispatchIntervalInSeconds, seconds)
                self.timer = Timer.scheduledTimer(withTimeInterval: Double(dispatchInterval), repeats: false, block: { [weak self] _ in
                    self?.process(operation, after: 0)
                })
            }
        } else {
            log.error("No dispatchIntervalInSeconds set. Not repeating operation")
        }
        
    }
    
    public func stopRepeat() {
        self.stateQueue.sync {
            self.started = false
            self.operationQueue.cancelAllOperations()
        }
        self.timerQueue.sync {
            self.timer?.invalidate()
        }
    }
}
