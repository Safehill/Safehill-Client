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
        case unauthenticated
        case notFound
        
        public func toCode() -> Int {
            switch self {
            case .unauthenticated:
                return 401
            case .notFound:
                return 405
            default:
                return 400
            }
        }
    }
    
    public enum ServerError : Error {
        case generic(String)
        case notImplemented
        case outdatedKeys
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
