import Foundation

extension Error {
    
    var isNetworkUnavailable: Bool {
        if self is URLError {
            return true
        }
        if self is SHHTTPError.TransportError {
            return true
        }
        return false
    }
}
