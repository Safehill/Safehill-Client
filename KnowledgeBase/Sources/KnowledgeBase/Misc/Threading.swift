//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/11/21.
//

import Foundation

let kKBDispatchSemaphoreMaxTimeoutInMilliseconds = 15000
internal var kKBDispatchSemaphoreDefaultValue = 0
internal var KBDispatchSemaphoreDefaultTimeout: DispatchTime {
    return .now() + .milliseconds(kKBDispatchSemaphoreMaxTimeoutInMilliseconds)
}

protocol Initiable { init() }

extension Dictionary: Initiable {}
extension Array: Initiable {}
extension Optional : Initiable {
    init() {
        self = nil
    }
}
extension Int : Initiable {
    init() {
        self = 0
    }
}

extension Bool : Initiable {
    init() {
        self = false
    }
}


public class KBTimedDispatch {
    let queue: DispatchQueue
    public let semaphore: DispatchSemaphore
    
    var _interruptError: Error? = nil
    let _timeout: DispatchTime
    var _group: DispatchGroup?
    
    public var group: DispatchGroup {
        if self._group == nil {
            self._group = DispatchGroup()
        }
        return self._group!
    }
    
    init(timeout: DispatchTime = KBDispatchSemaphoreDefaultTimeout) {
        self._timeout = timeout
        self.queue = DispatchQueue(
            label: "\(KnowledgeBaseBundleIdentifier).KBDispatch", attributes: .concurrent)
        self.semaphore = DispatchSemaphore(value: kKBDispatchSemaphoreDefaultValue)
    }
    
    public convenience init(timeoutInMilliseconds: Int) {
        let timeout = DispatchTime.now() + .milliseconds(timeoutInMilliseconds)
        self.init(timeout: timeout)
    }
    
    public convenience init() {
        self.init(timeout: KBDispatchSemaphoreDefaultTimeout)
    }
    
    public func interrupt(_ error: Error) {
        self._interruptError = error
        self.semaphore.signal()
    }
    
    public func wait() throws {
        if self._group != nil {
            self._group!.notify(queue: queue) {
                self.semaphore.signal()
            }
        }
        
        let dispatchResult = self.semaphore.wait(timeout: self._timeout)
        if case .timedOut = dispatchResult {
            throw KBError.timeout
        }
        if let e = self._interruptError {
            throw e
        }
    }
    
    public func notify(_ execute: @escaping () -> Void) throws {
        if self._group != nil {
            self._group!.notify(queue: queue) {
                execute()
                self.semaphore.signal()
            }
        } else {
            throw KBError.notSupported
        }
    }
}


internal func KBSyncMethodReturningVoid(execute asyncMethod: @escaping (@escaping (Swift.Result<Void, Error>) -> ()) -> ()) throws {
    try KBSyncMethodReturningVoid(value: kKBDispatchSemaphoreDefaultValue,
                                  timeout: KBDispatchSemaphoreDefaultTimeout,
                                  execute: asyncMethod)
}

internal func KBSyncMethodReturningVoid(value: Int,
                                        timeout: DispatchTime,
                                        execute: @escaping (@escaping (Swift.Result<Void, Error>) -> ()) -> ()) throws {
    var error: Error? = nil
    
    let semaphore = DispatchSemaphore(value: value)
    execute {
        result in
        switch result {
        case .failure(let err):
            error = err
            semaphore.signal()
        case .success():
            semaphore.signal()
        }
    }
    
    if case .timedOut = semaphore.wait(timeout: timeout) {
        throw KBError.timeout
    }
    if let _ = error {
        throw error!
    }
}

internal func KBSyncMethodReturningInitiable<T: Initiable>(execute asyncMethod: @escaping (@escaping (Swift.Result<T, Error>) -> ()) -> ()) throws -> T {
    return try KBSyncMethodReturningInitiable(value: kKBDispatchSemaphoreDefaultValue,
                                              timeout: KBDispatchSemaphoreDefaultTimeout,
                                              execute: asyncMethod)
}

internal func KBSyncMethodReturningInitiable<T: Initiable>(value: Int,
                                                           timeout: DispatchTime,
                                                           execute: @escaping (@escaping (Swift.Result<T, Error>) -> ()) -> ()) throws -> T {
    var error: Error? = nil
    var result = T.init()
    
    let semaphore = DispatchSemaphore(value: value)
    execute {
        r in
        switch r {
        case .failure(let err):
            error = err
        case .success(let res):
            result = res
        }
        semaphore.signal()
    }
    
    if case .timedOut = semaphore.wait(timeout: timeout) {
        throw KBError.timeout
    }
    if let _ = error {
        throw error!
    }
    
    return result
}
