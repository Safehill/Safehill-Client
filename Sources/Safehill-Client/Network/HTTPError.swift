import Foundation

public enum SHHTTPError {
    public enum ClientError : Error, LocalizedError {
        case badRequest(String)
        case unauthorized
        case paymentRequired
        case notFound
        case methodNotAllowed
        case conflict
        
        public func toCode() -> Int {
            switch self {
            case .unauthorized:
                return 401
            case .paymentRequired:
                return 402
            case .notFound:
                return 404
            case .methodNotAllowed:
                return 405
            case .conflict:
                return 409
            default:
                return 400
            }
        }
        
        public var errorDescription: String? {
            switch self {
            case .badRequest(let message):
                return message
            case .unauthorized:
                return "401 Unauthorized"
            case .conflict:
                return "409 Conflict"
            case .paymentRequired:
                return "402 PaymentRequired"
            default:
                return "Error \(self.toCode())"
            }
        }
    }
    
    public enum ServerError : Error, LocalizedError {
        case generic(String)
        case notImplemented
        case noData
        case unexpectedResponse(String)
        case badGateway
        
        public func toCode() -> Int {
            switch self {
            case .notImplemented:
                return 501
            case .badGateway:
                return 503
            default:
                return 500
            }
        }
        
        public var errorDescription: String? {
            switch self {
            case .generic(let message):
                return message
            case .badGateway:
                return "The route doesn't exist on the server (yet)"
            case .unexpectedResponse(let message):
                return "Unexpected response from server: \(message)"
            case .noData:
                return "Server returned no data"
            case .notImplemented:
                return "This functionality is not implemented yet"
            }
        }
    }
    
    public enum TransportError : Error {
        case generic(Error)
        case timedOut
    }
}
