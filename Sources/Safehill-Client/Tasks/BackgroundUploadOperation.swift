//
//  File.swift
//  
//
//  Created by Gennaro on 23/02/22.
//

import Foundation
import KnowledgeBase

public protocol SHBackgroundUploadOperationProtocol {
    func content(ofQueueItem item: KBQueueItem) throws -> SHGroupableUploadQueueItem
}
