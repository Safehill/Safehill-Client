//
//  XPCProtocol.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/27/21.
//

import Foundation

public class KnowledgeBaseXPCUtils : NSObject {
}


@objc extension KnowledgeBaseXPCUtils {
    public class func KBServiceXPCInterface() -> NSXPCInterface {
        let interface = NSXPCInterface(with: KBStorageXPCProtocol.self)

        let allowedValues = BlobValueAllowedClasses + [
            NSNull.self,
            NSString.self,
            NSNumber.self,
            NSArray.self,
            NSDictionary.self
        ]
        
        // MARK: keys(inStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keys(inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSArray.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keys(inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)

        // MARK: keys(matching:inStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [KBGenericCondition.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keys(matching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keys(matching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSArray.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keys(matching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)

        // MARK: value(forKey:inStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.value(forKey:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.value(forKey:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: false)
        interface.setClasses(NSSet(array: allowedValues) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.value(forKey:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)

        // MARK: keysAndValues(inStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keysAndValues(inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSDictionary.self, NSString.self] + allowedValues) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keysAndValues(inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)

        // MARK: keysAndValues(forKeysMatching:inStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [KBGenericCondition.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keysAndValues(forKeysMatching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keysAndValues(forKeysMatching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSDictionary.self, NSString.self] + allowedValues) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keysAndValues(forKeysMatching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)
        
        // MARK: keysAndValues(createdWithin:limit:order:inStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [DateInterval.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keysAndValues(createdWithin:limit:order:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keysAndValues(createdWithin:limit:order:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 3,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSDate.self, NSDictionary.self, NSString.self] + allowedValues) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.keysAndValues(createdWithin:limit:order:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)

        // MARK: tripleComponents(matching:inStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [KBTripleCondition.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.tripleComponents(matching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.tripleComponents(matching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSArray.self, KBTriple.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.tripleComponents(matching:inStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)

        // MARK: save(_:toStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [NSDictionary.self, NSString.self] + allowedValues) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.save(_:toStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.save(_:toStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: false)

        // MARK: save(_:toSynchedStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [NSDictionary.self, NSString.self] + allowedValues) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.save(_:toSynchedStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.save(_:toSynchedStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: false)

        // MARK: removeValues(forKeysMatching:fromStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [KBGenericCondition.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeValues(forKeysMatching:fromStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeValues(forKeysMatching:fromStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSArray.self, NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeValues(forKeysMatching:fromStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)

        // MARK: removeValues(forKeysMatching:fromSynchedStoreWithIdentifier:completionHandler:
        interface.setClasses(NSSet(array: [KBGenericCondition.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeValues(forKeysMatching:fromSynchedStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeValues(forKeysMatching:fromSynchedStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSArray.self, NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeValues(forKeysMatching:fromSynchedStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)
        
        // MARK: removeAll(fromStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeAll(fromStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSArray.self, NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeAll(fromStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)
        
        // MARK: removeAll(fromSynchedStoreWithIdentifier:completionHandler:)
        interface.setClasses(NSSet(array: [NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeAll(fromSynchedStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 0,
                             ofReply: false)
        interface.setClasses(NSSet(array: [NSArray.self, NSString.self]) as! Set<AnyHashable>,
                             for: #selector(KBStorageXPCProtocol.removeAll(fromSynchedStoreWithIdentifier:completionHandler:)),
                             argumentIndex: 1,
                             ofReply: true)

        return interface
    }
}
