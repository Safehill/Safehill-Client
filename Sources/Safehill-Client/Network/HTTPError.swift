//
//  File.swift
//  
//
//  Created by Gennaro on 04/12/21.
//

import Foundation

public enum SHHTTPError {
    public enum ClientError : Error {
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
    }
    
    public enum ServerError : Error {
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
    }
    
    public enum TransportError : Error {
        case generic(Error)
    }
}
