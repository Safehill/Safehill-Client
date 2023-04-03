//
//  Errors.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation

public enum KBError: CustomNSError, LocalizedError {
    case timeout
    case notSupported
    case databaseNotReady
    case databaseException(String)
    case fatalError(String)
    case unexpectedData(Any?)
    case genericError(Int)
    
    public static var errorDomain: String {
        return KnowledgeBaseBundleIdentifier
    }
    
    public var errorCode: Int {
        switch self {
        case .timeout: return 100
        case .notSupported: return 101
        case .databaseNotReady: return 102
        case .databaseException(_): return 103
        case .fatalError(_): return 104
        case .unexpectedData(_): return 105
        case .genericError(let code): return code
        }
    }
    
    public var errorDescription: String? {
        switch self {
        case .timeout: return "The operation timed out"
        case .notSupported: return "This operation is not supported"
        case .databaseNotReady: return "Could not access database"
        case .databaseException(let s): return "The database threw an exception: \(s)"
        case .fatalError(let s): return "A fatal error occurred: \(s)"
        case .unexpectedData(let data): return "Unexpected data: \(String(describing: data))"
        case .genericError(let code): return "Error with code: \(code)"
        }
    }
    
    public var errorUserInfo: [String : Any] {
        return ["localizedDescription": self.errorDescription ?? ""]
    }
}
