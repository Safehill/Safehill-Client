//
//  KVStore.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/16/21.
//

import Foundation

let KnowledgeBaseInMemoryIdentifier = ":memory:"
let KnowledgeBaseUserDefaultsIdentifier = ":userDefaults:"
let KnowledgeBaseSQLDefaultIdentifier = "main"
let KnowledgeBaseSQLSynchedIdentifier = "synched"


/// Protocol used for any serializable object
protocol Serializable {
    var encoded: String { get }
    static func decode(_ encoded: String) throws -> Self
}

@objc(KBStoreDelegate)
public protocol KBStoreDelegate {
    func kvDataDidChange(addedKeys: [String], removedKeys: [String])
    func kvWasDestroyed()
    func linkedDataDidChange()
}


@objc(KBKVStore)
open class KBKVStore : NSObject {

    /// The physical location of the KVS.
    public enum Location : Serializable, CustomStringConvertible {

        /// An in-memory database (equivalent to `.sql(":memory:")`).
        case inMemory

        /// An UserDefaults database (equivalent to `.sql(":userDefaults:")`).
        case userDefaults

        /// A SQL database with given name.
        /// :param: name The name of the store
        case sql(String)
        
        /// An SQL database, shared across devices using CloudKit.
        /// :param: name The name of the store (local and remote names will match)
        case sqlSynched(String)
        
        public var description: String {
            return self.encoded
        }

        var encoded: String {
            switch self {
            case .inMemory: return KnowledgeBaseInMemoryIdentifier
            case .userDefaults: return KnowledgeBaseUserDefaultsIdentifier
            case .sql(let s): return "SQL::\(s)"
            case .sqlSynched(_): return KnowledgeBaseSQLSynchedIdentifier
            }
        }

        static func decode(_ encoded: String) throws -> Location {
            let components = encoded.components(separatedBy: "::")
            if components.count < 2 {
                let error = KBError.unexpectedData(encoded)
                throw error
            }
            let (prefix, name) = (components[0], components[1])
            switch prefix {
            case KnowledgeBaseSQLSynchedIdentifier: return .sqlSynched("")
            case "SQL": return .sql(name)
            case KnowledgeBaseInMemoryIdentifier: return .inMemory
            case KnowledgeBaseUserDefaultsIdentifier: return .userDefaults
            default: return .inMemory
            }
        }
    }

    @objc open var delegate: KBStoreDelegate?
    
    internal let backingStore: KBBackingStore
    public let location: Location

    @objc open var name: String {
        return self.backingStore.name
    }
    
    @objc public static var defaultBaseURL: URL? {
        KBSQLBackingStore.baseURL()
    }
 
    @objc open var baseURL: URL? {
        if let backingStore = self.backingStore as? KBSQLBackingStore {
            return backingStore.baseURL
        }
        return nil
    }
    
    @objc open var fullURL: URL? {
        return self.baseURL?
            .appendingPathComponent(self.name)
            .appendingPathExtension(DatabaseExtension)
    }
    
    // MARK: NSObjectProtocol, Hashable, Equatable

    @objc open override var hash: Int {
        return self.name.hashValue
    }
    @objc open override func isEqual(_ object: Any?) -> Bool {
        if let object = object as? KBKVStore {
            return self.name == object.name
        }
        return false
    }

    // MARK: Constructors / Destructors

    @objc open class func defaultStore() -> KBKVStore {
        return KBKVStore.store(withName: "")
    }
    
    @objc open class func defaultSynchedStore() -> KBKVStore {
        return KBKVStore.synchedStore(withName: "")
    }

    @objc open class func inMemoryStore() -> KBKVStore {
        return KBKVStore.store(Location.inMemory)
    }

    @objc open class func userDefaultsStore() -> KBKVStore {
        return KBKVStore.store(Location.userDefaults)
    }
    
    @objc open class func store(withName name: String) -> KBKVStore {
        if name == KnowledgeBaseInMemoryIdentifier {
            return KBKVStore.store(.inMemory)
        } else if name == KnowledgeBaseUserDefaultsIdentifier {
            return KBKVStore.store(.userDefaults)
        }
        return KBKVStore.store(Location.sql(name))
    }
    
    @objc open class func synchedStore(withName name: String) -> KBKVStore {
        return KBKVStore(Location.sqlSynched(name))
    }

    open class func store(_ location: Location) -> KBKVStore {
        return KBKVStore(location)
    }

    init(_ location: Location) {
        self.location = location

        switch (self.location) {
        case .inMemory, .sql(KnowledgeBaseInMemoryIdentifier):
            log.debug("using KBInMemoryBackingStore")
            self.backingStore = KBInMemoryBackingStore()
        case .userDefaults, .sql(KnowledgeBaseUserDefaultsIdentifier):
            log.debug("using KBUserDefaultsBackingStore")
            self.backingStore = KBUserDefaultsBackingStore()
#if os(macOS) && !DEBUG // Only use XPC on macOS in RELEASE mode
        case .sql(""):
            log.debug("using KBSQLXPCBackingStore")
            self.backingStore = KBSQLXPCBackingStore.mainInstance()
        case .sql(let name):
            log.debug("using KBSQLXPCBackingStore with name \(name)")
            self.backingStore = KBSQLXPCBackingStore(name: name)
        case .sqlSynched(""):
            log.debug("using KBCloudKitSQLXPCBackingStore")
            self.backingStore = KBCloudKitSQLXPCBackingStore.mainInstance()
        case .sqlSynched(let name):
            log.error("creating named sqlSynched database is not supported. \(name, privacy: .public)")
            log.debug("using KBCloudKitSQLXPCBackingStore")
            self.backingStore = KBCloudKitSQLXPCBackingStore.mainInstance()
#else
        case .sql(""):
            log.debug("using KBSQLBackingStore")
            self.backingStore = KBSQLBackingStore.mainInstance()
        case .sql(let name):
            log.debug("using KBSQLBackingStore with name \(name)")
            self.backingStore = KBSQLBackingStore(name: name)
        case .sqlSynched(""):
            log.debug("using KBCloudKitSQLBackingStore")
            self.backingStore = KBCloudKitSQLBackingStore.mainInstance()
        case .sqlSynched(let name):
            log.error("creating named sqlSynched database is not supported. \(name, privacy: .public)")
            log.debug("using KBCloudKitSQLBackingStore")
            self.backingStore = KBCloudKitSQLBackingStore.mainInstance()
#endif
        }
    }
    
    /// Only used for debugging
    /// - Parameter existingDB: URL to the existing DB
    internal init(existingDB: URL) {
        let name = existingDB.deletingPathExtension().lastPathComponent
        self.location = .sql(name)
        self.backingStore = KBSQLBackingStore(name: name,
                                              baseURL: existingDB.deletingLastPathComponent())
    }
    
    /**
     Check if value supports secure coding (is conform to NSSecureCoding).
     The value nil is considered conformed.
     
     - parameter value: value to check
     */
    func supportsSecureCoding(_ value: Any?) -> Bool {
        if let v = value {
            return ((v as? NSSecureCoding) != nil)
        }
        
        return true
    }
    
    open func writeBatch() -> KBKVStoreWriteBatch {
        return self.backingStore.writeBatch()
    }
}
