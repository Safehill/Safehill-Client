//
//  WriteBatch.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import Foundation

// TODO: Remove this once either Swift 5.5 supports exposing asynchronous API with completion handlers, or when the support for pre-Swift5.5 is dropped.
// This is a simple port of the code in Storage_Pre_Swift_5_5/Protocols/WriteBatch.swift
extension KBSQLWriteBatch {
    func write(completionHandler: @escaping KBActionCompletion) {
        guard let backingStore = self.backingStore as? KBSQLBackingStoreProtocol else {
            log.fault("KBSQLWriteBatch should back a KBSQLBackingStoreProtocol")
            completionHandler(.failure(KBError.notSupported))
            return
        }
        
        // Write buffer into the store
        // No need to arbitrate writes through the XPC service for SQL stores (only SQLXPC do)
        var unwrappedBuffer = Dictionary<String, Any>()
        for (k, v) in self.buffer {
            unwrappedBuffer[k] = nilToNSNull(v)
        }
        
        do {
            try backingStore.sqlHandler.save(keysAndValues: unwrappedBuffer)
            self.buffer.removeAll()
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
}



@objc public protocol KBKVStoreWriteBatch {
    func set(value: Any?, for key: String)
    func write() async throws
}

class KBAbstractWriteBatch {
    var buffer: Dictionary<String, Any?>
    let backingStore: KBBackingStore
    
    init(backingStore: KBBackingStore) {
        self.buffer = Dictionary<String, Any?>()
        self.backingStore = backingStore
    }
    
    @objc func set(value: Any?, for key: String) {
        self.buffer[key] = value
    }
}

class KBSQLWriteBatch : KBAbstractWriteBatch, KBKVStoreWriteBatch {
    func write() async throws {
        guard let backingStore = self.backingStore as? KBSQLBackingStoreProtocol else {
            log.fault("KBSQLWriteBatch should back a KBSQLBackingStoreProtocol")
            throw KBError.notSupported
        }
        
        // Write buffer into the store
        // No need to arbitrate writes through the XPC service for SQL stores (only SQLXPC do)
        var unwrappedBuffer = Dictionary<String, Any>()
        for (k, v) in self.buffer {
            unwrappedBuffer[k] = nilToNSNull(v)
        }
        
        try backingStore.sqlHandler.save(keysAndValues: unwrappedBuffer)
        self.buffer.removeAll()
    }
}

class KBUserDefaultsWriteBatch : KBAbstractWriteBatch, KBKVStoreWriteBatch {
    func write() async throws {
        guard let backingStore = self.backingStore as? KBUserDefaultsBackingStore else {
            log.fault("KBUserDefaultsWriteBatch should back a KBUserDefaultsBackingStore")
            throw KBError.notSupported
        }
        
        for key in self.buffer.keys {
            if let value = self.buffer[key] {
                try await backingStore.set(value: value, for: key)
            }
        }
        
        backingStore.synchronize()
        self.buffer.removeAll()
    }
}

class KBCloudKitSQLWriteBatch : KBSQLWriteBatch {
    override func write() async throws {
        try await super.write()
        // TODO: Trigger iCloud SYNC?
    }
}

#if !os(macOS)
// Use XPC only on macOS

class KBSQLXPCWriteBatch : KBSQLWriteBatch {
}

#else

class KBSQLXPCWriteBatch : KBAbstractWriteBatch, KBKVStoreWriteBatch {
    func write() async throws {
        guard let backingStore = self.backingStore as? KBSQLXPCBackingStore else {
            log.fault("KBSQLWriteBatch should back a KBSQLXPCBackingStore")
            throw KBError.notSupported
        }
        
        guard let daemon = backingStore.daemon() else {
            throw KBError.fatalError("Could not connect to XPC service")
        }
        
        // Transform nil to NSNull so that Dictionary<String, Any?>
        // becomes a Dictionary<String, Any!>, and can be converted to an NSDictionary (what the method save below expects)
        var unwrappedBuffer = Dictionary<String, Any>()
        for (k, v) in self.buffer {
            unwrappedBuffer[k] = nilToNSNull(v)
        }
        
        try await daemon.save(unwrappedBuffer, toStoreWithIdentifier: backingStore.name)
        self.buffer.removeAll()
    }
}
#endif

#if !os(macOS)
// Use XPC only on macOS

class KBCloudKitSQLXPCWriteBatch : KBSQLXPCWriteBatch {
}

#else
class KBCloudKitSQLXPCWriteBatch : KBSQLXPCWriteBatch {
    
    override func write() async throws {
        guard let backingStore = self.backingStore as? CKCloudKitBackingStore else {
            log.fault("KBCloudKitWriteBatch should back a CKCloudKitBackingStore")
            completionHandler(KBError.notSupported)
            return
        }
        
        try await super.write()
    }
}

#endif
