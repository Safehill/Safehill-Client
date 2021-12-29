//
//  File.swift
//  
//
//  Created by Gennaro on 04/12/21.
//

import Foundation

public enum SHHTTPError {
    public enum ClientError : Error, LocalizedError {
        case badRequest(String)
        case unauthorized
        case notFound
        case methodNotAllowed
        case conflict
        
        public func toCode() -> Int {
            switch self {
            case .unauthorized:
                return 401
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
                return "Invalid credentials"
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
        
        public func toCode() -> Int {
            switch self {
            case .notImplemented:
                return 501
            default:
                return 500
            }
        }
        
        public var errorDescription: String? {
            switch self {
            case .generic(let message):
                return message
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
    }
}
