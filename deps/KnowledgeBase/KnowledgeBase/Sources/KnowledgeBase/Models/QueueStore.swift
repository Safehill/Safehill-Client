//
//  QueueStore.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/4/21.
//

import Foundation

@objc(KBQueueStore)
public class KBQueueStore : KBKVStore {
    
    public enum QueueType {
        /// First in - first out
        case fifo
        /// Last in - first out
        case lifo
    }

    public let queueType: QueueType
    
    @available(*, unavailable)
    override init(_ location: Location) {
        fatalError("\(#function) is not supported for objects of type KBQueueStore")
    }
    
    @available(*, unavailable)
    @objc public override class func defaultStore() -> KBQueueStore {
        fatalError("\(#function) is not supported for objects of type KBQueueStore")
    }
    
    @available(*, unavailable)
    @objc public override class func defaultSynchedStore() -> KBQueueStore {
        fatalError("\(#function) is not supported for objects of type KBQueueStore")
    }

    @available(*, unavailable)
    @objc public override class func inMemoryStore() -> KBQueueStore {
        fatalError("\(#function) is not supported for objects of type KBQueueStore")
    }

    @available(*, unavailable)
    @objc public override class func userDefaultsStore() -> KBQueueStore {
        fatalError("\(#function) is not supported for objects of type KBQueueStore")
    }
    
    @available(*, unavailable)
    public override class func store(withName name: String) -> KBQueueStore {
        fatalError("\(#function) is not supported for objects of type KBQueueStore")
    }
    
    @available(*, unavailable)
    public override class func store(_ location: Location) -> KBQueueStore {
        fatalError("\(#function) is not supported for objects of type KBQueueStore")
    }
    
    @available(*, unavailable)
    @objc public override class func synchedStore(withName name: String) -> KBQueueStore {
        fatalError("\(#function) is not supported for objects of type KBQueueStore")
    }
    
    init(_ location: Location, type: QueueType) {
        self.queueType = type
        super.init(location)
    }
    
    public class func defaultStore(type: QueueType) -> KBQueueStore {
        return KBQueueStore.store(withName: "", type: type)
    }
    
    public class func defaultSynchedStore(type: QueueType) -> KBQueueStore {
        return KBQueueStore.synchedStore(withName: "", type: type)
    }

    public class func inMemoryStore(type: QueueType) -> KBQueueStore {
        return KBQueueStore.store(Location.inMemory, type: type)
    }

    public class func userDefaultsStore(type: QueueType) -> KBQueueStore {
        return KBQueueStore.store(Location.userDefaults, type: type)
    }
    
    public class func store(withName name: String, type: QueueType) -> KBQueueStore {
        if name == KnowledgeBaseInMemoryIdentifier {
            return KBQueueStore.store(.inMemory, type: type)
        } else if name == KnowledgeBaseUserDefaultsIdentifier {
            return KBQueueStore.store(.userDefaults, type: type)
        }
        return KBQueueStore.store(Location.sql(name), type: type)
    }
    
    public class func store(_ location: Location, type: QueueType) -> KBQueueStore {
        return KBQueueStore(location, type: type)
    }
    
    public class func synchedStore(withName name: String, type: QueueType) -> KBQueueStore {
        return KBQueueStore.store(Location.sqlSynched(name), type: type)
    }
}
