//
//  DataVault.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/25/21.
//

import Foundation

struct KBDataVault {
    static func createDirectory(at path: String) throws {
        let result = Swift.Result { try FileManager.default.createDirectory(
            atPath: path,
            withIntermediateDirectories: true,
            attributes: nil)
        }
        if case let .failure(error) = result {
            throw KBError.fatalError("Error creating directory at path \(path): \(error)")
        }
        
        try (NSURL(fileURLWithPath: path, isDirectory: true)).setResourceValue(URLFileProtection.complete,
                                                                               forKey: .fileProtectionKey)
    }
    
    static func encryptFile(at url: URL) throws {
        try (url as NSURL).setResourceValue(URLFileProtection.complete,
                                            forKey: .fileProtectionKey)
    }
}
