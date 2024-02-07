import Foundation

class CircuitBreaker {
    
    enum State {
        case Closed
        case Open
        case HalfOpen
    }
    
    let timeout: TimeInterval
    let maxRetries: Int
    let timeBetweenRetries: TimeInterval
    let exponentialBackoff: Bool
    let resetTimeout: TimeInterval
    var call: ((CircuitBreaker) -> Void)?
    var didTrip: ((CircuitBreaker, Error?) -> Void)?
    private(set) var failureCount = 0
    
    var state: State {
        if let lastFailureTime = lastFailureTime,
           failureCount > maxRetries,
           (Date().timeIntervalSince1970 - lastFailureTime) > resetTimeout {
            return .HalfOpen
        }
        
        if failureCount > maxRetries {
            return .Open
        }
        
        return .Closed
    }
    
    private var lastError: Error?
    private var lastFailureTime: TimeInterval?
    private var timer: Timer?
    
    init(
        timeout: TimeInterval = 10,
        maxRetries: Int = 2,
        timeBetweenRetries: TimeInterval = 2,
        exponentialBackoff: Bool = true,
        resetTimeout: TimeInterval = 10) {
            self.timeout = timeout
            self.maxRetries = maxRetries
            self.timeBetweenRetries = timeBetweenRetries
            self.exponentialBackoff = exponentialBackoff
            self.resetTimeout = resetTimeout
    }
    
    // MARK: - Public API
    
    func execute() {
        timer?.invalidate()
        
        switch state {
        case .Closed, .HalfOpen:
            doCall()
        case .Open:
            trip()
        }
    }
    
    func success() {
        reset()
    }
    
    func failure(_ error: Error? = nil) {
        timer?.invalidate()
        lastError = error
        lastFailureTime = NSDate().timeIntervalSince1970
        failureCount += 1
        
        switch state {
        case .Closed, .HalfOpen:
            retryAfterDelay()
        case .Open:
            trip()
        }
    }
    
    func reset() {
        timer?.invalidate()
        failureCount = 0
        lastFailureTime = nil
        lastError = nil
    }
    
    // MARK: - Call & Timeout
    
    private func doCall() {
        call?(self)
        startTimer(delay: timeout, selector: #selector(didTimeout(timer:)))
    }
    
    @objc private func didTimeout(timer: Timer) {
        failure()
    }
    
    // MARK: - Retry
    
    private func retryAfterDelay() {
        let timeBR = timeBetweenRetries > 1 ? timeBetweenRetries : (1 + timeBetweenRetries)
        let delay = exponentialBackoff ? pow(timeBR, Double(failureCount)) : timeBetweenRetries
        startTimer(delay: delay, selector: #selector(shouldRetry(timer:)))
    }
    
    @objc private func shouldRetry(timer: Timer) {
        doCall()
    }
    
    // MARK: - Trip
    
    private func trip() {
        didTrip?(self, lastError)
    }
    
    // MARK: - Timer
    
    private func startTimer(delay: TimeInterval, selector: Selector) {
        timer?.invalidate()
        DispatchQueue.main.async { [self] in
            timer = Timer.scheduledTimer(
                timeInterval: delay,
                target: self,
                selector: selector,
                userInfo: nil,
                repeats: false
            )
        }
    }
    
}
