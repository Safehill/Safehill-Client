//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/14/21.
//

import Foundation

@objc(KBPermanentEventStore)
open class KBPermanentEventStore : NSObject {
    
    internal let knowledgeStore: KBKnowledgeStore
    
    @objc public init(knowledgeStore: KBKnowledgeStore) {
        self.knowledgeStore = knowledgeStore
    }
    
    @objc public static func defaultStore() -> KBPermanentEventStore {
        return KBPermanentEventStore(knowledgeStore: KBKnowledgeStore.defaultStore() as! KBKnowledgeStore)
    }
}

@objc(KBPermanentReadableEventStore)
public protocol KBPermanentReadableEventStore {
    
    /**
     Retrieves the historic event given its identifier, if any is available.
     - parameter identifier: the event identifier
     */
    func historicEvent(withIdentifier identifier: String) async throws -> KBHistoricEvent?
}

extension KBPermanentEventStore : KBPermanentReadableEventStore {

    open func historicEvent(withIdentifier identifier: String) async throws -> KBHistoricEvent? {
        let rawEvent = try await self.knowledgeStore.value(forKey: identifier)
        if rawEvent == nil {
            return nil
        } else if let event = rawEvent as? KBHistoricEvent {
            return event
        } else {
            throw KBError.unexpectedData(rawEvent)
        }
    }
}


@objc(KBPermanentWritableEventStore)
public protocol KBPermanentWritableEventStore {
    /**
     Update the event store with the event provided.
     
     Multiple calls to this method with the same will result in an increment of the event frequency,
     totalDuration, etc. in the store.
     
     - parameter event: The event to record
     */
    @objc(recordEvent:completionHandler:)
    func record(_ event: KBEvent) async throws
}

extension KBPermanentEventStore : KBPermanentWritableEventStore {
    
    @objc(createEventWithIdentifier:dateInterval:metadata:fromEvent:)
    public static func createEvent(withIdentifier identifier: String,
                                   dateInterval: DateInterval?,
                                   metadata: KBJSONObject,
                                   from event: KBHistoricEvent? = nil) -> KBHistoricEvent {
        let seenDate = dateInterval?.start ?? Date()
        let interval = dateInterval?.duration ?? 0
        var firstSeen = seenDate
        var frequency = 1
        var totalDuration = interval
        
        if let e = event {
            firstSeen = e.firstSeen
            frequency = e.frequency + 1
            totalDuration = e.totalDuration + interval
        }
        
        return KBHistoricEvent(identifier: identifier,
                               firstSeen: firstSeen,
                               lastSeen: seenDate,
                               frequency: frequency,
                               lastDuration: interval,
                               totalDuration: totalDuration,
                               metadata: metadata)
    
    }
    
    internal func createEventIfNotExists(withIdentifier identifier: String,
                                         dateInterval: DateInterval?,
                                         metadata: KBJSONObject) async throws -> KBHistoricEvent {
        let existingEvent = try await self.historicEvent(withIdentifier: identifier)
        let event = KBPermanentEventStore.createEvent(withIdentifier: identifier,
                                                      dateInterval: dateInterval,
                                                      metadata: metadata,
                                                      from: existingEvent)
        return event
    }
    
    public func record(_ event: KBEvent) async throws {
        let dateInterval = DateInterval(start: event.startDate,
                                        end: event.endDate)
        let historicEvent = try await self.createEventIfNotExists(withIdentifier: event.identifier,
                                                                  dateInterval: dateInterval,
                                                                  metadata: event.metadata as KBJSONObject)
        log.info("Updating the store with event (\(event)). New historic event is (\(historicEvent))")
        log.debug("event.metadata (\(event.metadata)) historicEvent.metadata (\(historicEvent.metadata))")
            
        try await self.knowledgeStore.set(value: historicEvent, for: historicEvent.identifier)
    }
}

