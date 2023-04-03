//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/14/21.
//

import Foundation

@objc(KBEphemeralReadableEventSequence)
public protocol KBEphemeralReadableEventSequence : AnyObject {
    /**
     Get the last occuring event
     - return : the last event based on their end date
     */
    func last() async throws -> KBEvent?
    
    /**
     Get the first occurring event
     - return : the first event based on their end date
     */
    func first() async throws -> KBEvent?
    
    /**
     Get the events with identifier equals to the input parameter
     - parameter identifier: the event identifier
     - return : all the events with that identifier
     */
    func events(withIdentifier identifier: String) async throws -> [KBEvent]
    
    /**
     Get the events with a specific date range
     - parameter startDate: events must have startDate greater than this value
     - parameter endDate: events must have endDate lower than this value
     - return : all the events in the specified range
     */
    func events(between startDate: Date, and endDate: Date) async throws -> [KBEvent]
}

extension KBEphemeralEventSequence : KBEphemeralReadableEventSequence {
    
    internal func findEntity(preceding startDate: Date) async throws -> KBEntity {
        var result: KBEntity?
        var prevEventEntities = [KBEntity]()
        
        let value = try await KBKnowledgeStore.inMemoryGraph.value(forKey: kKBEphemeralEventSequenceLastIdentifier)
        if let v = value as? String {
            prevEventEntities.append(KBKnowledgeStore.inMemoryGraph.entity(withIdentifier: v))
        }
        
        while prevEventEntities.count > 0 {
            result = prevEventEntities.first
            prevEventEntities.removeFirst()
            
            guard let prevDate: Date = try await result?.value(forKey: "startDate") as? Date else {
                throw KBError.unexpectedData(try await result?.value(forKey: "startDate"))
            }
            
            if prevDate.compare(startDate) == .orderedAscending {
                return result!
            }
            
            let otherPrevEvents = try await result?.linkingEntities(withPredicate: kKBEphemeralEventSequencePredicateLabel)
                .map { $0.subject }
                .filter { $0.identifier != kKBEphemeralEventSequenceStartEntityIdentifier }
            prevEventEntities.append(contentsOf: otherPrevEvents!)
        }
        
        return KBKnowledgeStore.inMemoryGraph.entity(withIdentifier: kKBEphemeralEventSequenceStartEntityIdentifier)
    }
    
    private func findEvents(withCondition condition: (KBEntity) -> Bool) async throws -> [KBEvent] {
        var result = [KBEvent]()
        
        var event: KBEntity
        var eventsToVisit = [KBKnowledgeStore.inMemoryGraph.entity(withIdentifier: kKBEphemeralEventSequenceStartEntityIdentifier)]
        
        while eventsToVisit.count > 0 {
            event = eventsToVisit.first!
            eventsToVisit.removeFirst()
            
            if condition(event) {
                result.append(try KBEvent(entity: event))
            }
            
            let otherEvents = try await event.linkedEntities(withPredicate: kKBEphemeralEventSequencePredicateLabel).map { $0.object }
            eventsToVisit.append(contentsOf: otherEvents)
        }
        
        return result
    }
    
    public func events(between startDate: Date, and endDate: Date) async throws -> [KBEvent] {
        return try await self.findEvents { (entity: KBEntity) -> Bool in
            guard let eventStartDate: Date = entity.value(forKey: "startDate") as? Date else {
                log.error("missing startDate for entity identifier \(entity.identifier, privacy: .private(mask: .hash))")
                return false
            }
            guard let eventEndDate: Date = entity.value(forKey: "endDate") as? Date else {
                log.error("missing endDate for entity identifier \(entity.identifier, privacy: .private(mask: .hash))")
                return false
            }
            
            return eventStartDate.compare(startDate) == .orderedDescending
                && eventEndDate.compare(endDate) == .orderedAscending
        }
    }
    
    public func events(withIdentifier identifier: String) async throws -> [KBEvent] {
        return try await self.findEvents { (entity: KBEntity) -> Bool in
            guard let eventIdentifier: String = entity.value(forKey: "identifier") as? String else {
                log.error("missing identifier for entity  identifier \(entity.identifier, privacy: .private(mask: .hash))")
                return false
            }
            
            return eventIdentifier == identifier
        }
    }
    
    public func first() async throws -> KBEvent? {
        let event = try await self.historyStartEvent.linkedEntities(withPredicate: kKBEphemeralEventSequencePredicateLabel)
            .map { $0.object }
            .sorted {
                (a, b) in
                let lhv = a.value(forKey: "startDate") as! Date
                let rhv = b.value(forKey: "startDate") as! Date
                return lhv.compare(rhv) == .orderedAscending
            }
            .first
        
        if event != nil {
            return try KBEvent(entity: event!)
        }
        
        return nil
    }
    
    public func last() async throws -> KBEvent? {
        let value = try await KBKnowledgeStore.inMemoryGraph.value(forKey: kKBEphemeralEventSequenceLastIdentifier)
        if let v = value as? String, v != kKBEphemeralEventSequenceStartEntityIdentifier {
            return try KBEvent(entity: KBKnowledgeStore.inMemoryGraph.entity(withIdentifier: v))
        }
        return nil
    }
}
