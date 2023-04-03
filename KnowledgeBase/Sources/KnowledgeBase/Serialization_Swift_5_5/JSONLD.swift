//
//  JSONLD.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import Foundation

public typealias KBKVPairs = [String: Any]

enum JSONLDParseError: Error {
    case unexpectedFormat, resourceNotAvailable
}

@objc(KBJSONLDGraph)
public class KBJSONLDGraph : NSObject {

    fileprivate let _entities: [KBEntity]

    @objc public init(withEntities entities: [KBEntity]?) {
        self._entities = entities ?? []
    }

    @objc public var entities: [String] {
        return self._entities.map { $0.identifier }
    }

    @objc public func linkedData() async throws -> [KBKVPairs] {
        var linkedDataDictionary = [KBKVPairs]()
        let entities = Array(Set(self._entities))
        
        for entity in entities {
            do {
                linkedDataDictionary.append(try await KBJSONLDGraph.serialize(entity))
            } catch {
                log.error("serialization of \(entity) failed. \(error.localizedDescription, privacy: .public)")
                throw error
            }
        }
        
        return linkedDataDictionary
    }

    private static func serialize(_ entity: KBEntity) async throws -> KBKVPairs {
        var object = KBKVPairs()
        //            object["@context"] = "http://schema.org/"
        object["@id"] = entity.identifier

        for (link, linkedEntity) in try await entity.linkedEntities() {
            object.append(link, value: linkedEntity.identifier)
        }
        
        return object
    }
}

// MARK: - CKKnowledgeStore + CKJSONLD

extension KBKnowledgeStore {
    
    @objc public func subgraph(withEntities identifiers: [Label]) -> KBJSONLDGraph {
        return KBJSONLDGraph(withEntities: identifiers.map {
            self.entity(withIdentifier: $0)
        })
    }
    
    internal func evaluateJSONLDEntry(forEntity entity: KBEntity,
                                      key: Any,
                                      value: Any) async throws {
        guard let _key = key as? String else {
            log.error("key=\(key as! String) is not a string. class = \(String(describing: type(of: key)), privacy: .public)")
            throw KBError.unexpectedData(key)
        }
        
        if _key == "@context" || _key == "@id" {
            return
        }
        
        if let stringValue = value as? String {
            try await entity.link(to: self.entity(withIdentifier: stringValue),
                                  withPredicate: _key)
        } else if let stringArrayValue = value as? [String] {
            for stringValue in stringArrayValue {
                let otherEntity = self.entity(withIdentifier: stringValue)
                try await entity.link(to: otherEntity, withPredicate: _key)
            }
        } else if let jsonObject = value as? KBKVPairs {
            let targetEntity = self.entity(withIdentifier: (jsonObject["@id"] ?? "_:\(UUID().uuidString)") as! String)
            
            for (k, v) in jsonObject {
                try await self.evaluateJSONLDEntry(forEntity: targetEntity, key: k, value: v)
            }
            
            try await entity.link(to: targetEntity,
                        withPredicate: _key)
        } else if let jsonObjects = value as? [KBKVPairs] {
            for jsonObject in jsonObjects {
                try await self.evaluateJSONLDEntry(forEntity: entity, key: key, value: jsonObject)
            }
        } else {
            throw KBError.notSupported
        }
    }
    
    fileprivate func `import`(entity: KBEntity,
                              fromJsonld jsonObject: Any) async throws {
        if let dictionary = jsonObject as? KBKVPairs {
            for (k, v) in dictionary {
                try await self.evaluateJSONLDEntry(forEntity: entity, key: k, value: v)
            }
        } else {
            throw JSONLDParseError.unexpectedFormat
        }
    }
    
    internal func importJSONLD(data: Data) async throws {
        let evaluate = {
            (object: KBKVPairs) async throws -> Void in
            let entity = self.entity(withIdentifier: (object["@id"] ?? "_:\(UUID().uuidString)") as! String)
            try await self.import(entity: entity, fromJsonld: object)
        }
        
        let object = try JSONSerialization.jsonObject(with: data, options: JSONSerialization.ReadingOptions())
        
        if let array = object as? [KBKVPairs] {
            for obj in array {
                try await evaluate(obj)
            }
        } else if let obj = object as? KBKVPairs {
            try await evaluate(obj)
        } else {
            throw KBError.unexpectedData(object)
        }
    }
    
    //MARK: importContentsOf(JSONLDFileAt:completionHandler:)
    
    @objc public func importContentsOf(JSONLDFileAt path: String) async throws {
        guard FileManager.default.fileExists(atPath: path) else {
            log.error("no such JSONLD file at path \(path, privacy: .public)")
            throw JSONLDParseError.resourceNotAvailable
        }
        
        let data = try Data(
            contentsOf: URL(fileURLWithPath: path),
            options: Data.ReadingOptions.alwaysMapped
        )
        try await self.importJSONLD(data: data)
    }
    
}

