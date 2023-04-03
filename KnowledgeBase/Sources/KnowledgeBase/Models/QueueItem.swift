//
//  QueueItem.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/4/21.
//

import Foundation

@objc(KBQueueItem)
public class KBQueueItem : NSObject {
    public let identifier: String
    public let content: Any
    public let createdAt: Date
    
    init(identifier: String, content: Any, createdAt: Date) {
        self.identifier = identifier
        self.content = content
        self.createdAt = createdAt
    }
    
    convenience init(item: KBKVPairs, createdAt: Date) throws {
        guard item.count == 1 else {
            throw KBError.unexpectedData(item)
        }
        self.init(identifier: item.first!.key,
                  content: item.first!.value,
                  createdAt: createdAt)
    }
}
