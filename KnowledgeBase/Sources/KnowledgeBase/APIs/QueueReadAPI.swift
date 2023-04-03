//
//  QueueReadAPI.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/4/21.
//

import Foundation

// MARK: - KBQueueStore Read API

func toQueueItems(itemsKeyedByDate: [Date: KBKVPairs]) throws -> [KBQueueItem] {
    var queueItems = [KBQueueItem]()
    for (date, item) in itemsKeyedByDate {
        do {
            queueItems.append(try KBQueueItem(item: item, createdAt: date))
        } catch {
            throw KBError.unexpectedData(item)
        }
    }
    return queueItems
}

extension KBQueueStore {
    
    /**
     Retrieves all queue items created within the provided time range, sorted in the order determined by the queue type (fifo, lifo)
     
     - parameter createdWithin: the time range
     - parameter limit: limits the number of items
     - parameter completionHandler: the callback method
     
     */
    open func peekItems(createdWithin interval: DateInterval,
                        limit: Int? = nil,
                        completionHandler: @escaping (Swift.Result<[KBQueueItem], Error>) -> ()) {
        let order = self.queueType == .fifo ? ComparisonResult.orderedAscending : ComparisonResult.orderedDescending
        return self.backingStore.dictionaryRepresentation(createdWithin: interval, limit: limit, order: order) { result in
            switch result {
            case .success(let itemsKeyedByDate):
                do {
                    let items = try toQueueItems(itemsKeyedByDate: itemsKeyedByDate).sorted { $0.createdAt > $1.createdAt }
                    completionHandler(.success(items))
                } catch {
                    completionHandler(.failure(error))
                    return
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    @objc open func peekItems(createdWithin interval: DateInterval,
                              limit: Int = -1,
                              completionHandler: @escaping (Error?, [KBQueueItem]) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler) {
            c in
            self.peekItems(createdWithin: interval, limit: limit, completionHandler: c)
        }
    }
    
    
    /**
     Retrieves all keys and values in the KVS, for pairs created within a time range, sorted in the order determined by the queue type (fifo, lifo)
     Blocking version.
     
     - parameter createdWithin: the time range
     - parameter limit: limits the number of items
     - returns: the list of items
     
     */
    open func peekItems(createdWithin interval: DateInterval, limit: Int? = nil) throws -> [KBQueueItem] {
        let order = self.queueType == .fifo ? ComparisonResult.orderedAscending : ComparisonResult.orderedDescending
        let itemsKeyedByDate = try self.backingStore.dictionaryRepresentation(createdWithin: interval, limit: limit, order: order)
        return try toQueueItems(itemsKeyedByDate: itemsKeyedByDate)
    }
    
    /**
     Retrieves the next item in the queue, according to the policy for the queue type (fifo, lifo)
     
     - parameter completionHandler: the callback method

     */
    open func peek(completionHandler: @escaping (Swift.Result<KBQueueItem?, Error>) -> ()) {
        self.peekItems(createdWithin: DateInterval(start: Date.distantPast, end: Date()),
                   limit: 1) { result in
            switch result {
            case .success(let resultByDate):
                completionHandler(.success(resultByDate.first))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    @objc open func peek(completionHandler: @escaping (Error?, KBQueueItem?) -> ()) {
        KBObjectiveCAPIResultReturningInitiable(completionHandler: completionHandler) {
            c in
            self.peek(completionHandler: c)
        }
    }
    
    /**
     Retrieves the next item in the queue, according to the policy for the queue type (fifo, lifo)
     Blocking version.
     
     - returns: the next item in the queue
     
     */
    open func peek() throws -> KBQueueItem? {
        return try self.peekItems(createdWithin: DateInterval(start: Date.distantPast, end: Date()), limit: 1).first
    }
}

