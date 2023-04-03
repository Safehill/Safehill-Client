//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/14/21.
//

import Foundation

@objc(KBEphemeralWritableEventSequence)
public protocol KBEphemeralWritableEventSequence : AnyObject {
    
    /**
     Add to the in-memory event sequence.
     Appending the same event twice is a no-op.
     
     - parameter event: the event to append to the sequence
     */
    func append(event: KBEvent) async throws
    
}

extension KBEphemeralEventSequence : KBEphemeralWritableEventSequence {
    
    open func append(event: KBEvent) async throws {
        let eventEntity = KBEntity(event: event, knowledgeStore: KBKnowledgeStore.inMemoryGraph)
        
        let condition = KBTripleCondition(subject: eventEntity.identifier, predicate: nil, object: nil)
            .or(KBTripleCondition(subject: nil, predicate: nil, object: eventEntity.identifier))
        
        // Identity check
        if try await KBKnowledgeStore.inMemoryGraph.triples(matching: condition).count > 0 {
            log.info("event \(event) already recorded")
        } else {
            let previousEventEntity: KBEntity = try await self.findEntity(preceding: event.startDate)
            
            // Update "precedes" links from previousEvent to "new" following event
            // Remove old precedes and set up new ones
            for followingLink in try await previousEventEntity.linkedEntities(withPredicate: kKBEphemeralEventSequencePredicateLabel) {
                try await previousEventEntity.unlink(to: followingLink.object, withPredicate: kKBEphemeralEventSequencePredicateLabel)
                try await eventEntity.link(to: followingLink.object, withPredicate: kKBEphemeralEventSequencePredicateLabel)
            }
            try await previousEventEntity.link(to: eventEntity,
                                         withPredicate: kKBEphemeralEventSequencePredicateLabel)
            
            // Update the KBEphemeralEventSequenceLastIdentifier if this event is the last one
            if try await eventEntity.linkedEntities(withPredicate: kKBEphemeralEventSequencePredicateLabel).count == 0 {
                try await KBKnowledgeStore.inMemoryGraph.set(value: eventEntity.identifier, for: kKBEphemeralEventSequenceLastIdentifier)
            }
        }
        
        try await eventEntity._setvalues(for: ["identifier": event.identifier,
                                                  "startDate": event.startDate,
                                                  "endDate": event.endDate,
                                                  "metadata": event.metadata])
    }

}

