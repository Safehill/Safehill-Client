//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/14/21.
//

import Foundation

let kKBEphemeralEventSequenceLastIdentifier = "eventSequenceLastIdentifier" // Keeps track of the last event in the sequence
let kKBEphemeralEventSequencePredicateLabel = "precedes"
let kKBEphemeralEventSequenceStartEntityIdentifier = "sequenceStart"

extension KBEntity {
    convenience init(event: KBEvent, knowledgeStore: KBKnowledgeStore) {
        let id = KBEphemeralEventSequence.JOINER.combine(event.identifier,
                                                         event.startDate.toString(KBDefaultDateFormat)!,
                                                         event.endDate.toString(KBDefaultDateFormat)!)
        self.init(identifier: id, knowledgeStore: knowledgeStore)
    }
}

public extension KBEvent {
    
    convenience init(entity: KBEntity) throws {
        guard let identifier = entity.value(forAttribute: "identifier") as? String else {
            log.error("Missing event identifier for entity \(entity.identifier, privacy: .private(mask: .hash))")
            throw KBError.unexpectedData(entity.value(forAttribute: "identifier"))
        }
        guard let startDate = entity.value(forAttribute: "startDate") as? Date else {
            log.error("Missing event startDate for entity \(entity.identifier, privacy: .private(mask: .hash))")
            throw KBError.unexpectedData(entity.value(forAttribute: "startDate"))
        }
        guard let endDate = entity.value(forAttribute: "endDate") as? Date else {
            log.error("Missing event endDate for entity identifier \(entity.identifier, privacy: .private(mask: .hash))")
            throw KBError.unexpectedData(entity.value(forAttribute: "endDate"))
        }
        
        var metadata = entity.value(forAttribute: "metadata") as? [String: Any]
        if metadata == nil {
            metadata = [String: Any]()
        }
        
        self.init(identifier: identifier,
                  start: startDate,
                  end: endDate,
                  metadata: metadata!)
    }
    
    /**
     Get the event immediately preceding this one (if any was recorded)
     - return : the event, or nil if none was found
     */
    func previous() async throws -> KBEvent? {
        let entity = try await KBEntity(event: self, knowledgeStore: KBKnowledgeStore.inMemoryGraph)
            .linkingEntities(withPredicate: kKBEphemeralEventSequencePredicateLabel).map { $0.subject }
            .sorted {
                (a, b) in
                let lhv = a.value(forAttribute: "startDate") as! Date
                let rhv = b.value(forAttribute: "startDate") as! Date
                return lhv.compare(rhv) == .orderedAscending
            }
            .last
        if let entity = entity {
            return try KBEvent(entity: entity)
        }
        return nil
    }
    
    /**
     Get the event immediately following this one (if any was recorded)
     - return : the event, or nil if none was found
     */
    func next() async throws -> KBEvent? {
        let entity = try await KBEntity(event: self, knowledgeStore: KBKnowledgeStore.inMemoryGraph)
            .linkedEntities(withPredicate: kKBEphemeralEventSequencePredicateLabel).map { $0.object }
            .sorted {
                (a, b) in
                let lhv = a.value(forAttribute: "startDate") as! Date
                let rhv = b.value(forAttribute: "startDate") as! Date
                return lhv.compare(rhv) == .orderedAscending
            }
            .first
        if let entity = entity {
            return try KBEvent(entity: entity)
        }
        return nil
    }
}

@objc(KBEphemeralEventSequence)
public class KBEphemeralEventSequence: NSObject {
    static let JOINER = "_"
    
    public override init() {
        super.init()
    }
    
    internal var historyStartEvent: KBEntity {
        return KBKnowledgeStore.inMemoryGraph.entity(withIdentifier: kKBEphemeralEventSequenceStartEntityIdentifier)
    }
}

