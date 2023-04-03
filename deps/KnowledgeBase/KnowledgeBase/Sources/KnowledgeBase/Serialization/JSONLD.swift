//
//  JSONLD.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation

public typealias KBKVPairs = [String: Any]

public class KBKVObjcPairWithTimestamp: NSObject {
    let key: String
    let value: Any
    let timestamp: Date
    
    init(key: String, value: Any, timestamp: Date) {
        self.key = key
        self.value = value
        self.timestamp = timestamp
    }
}

public struct KBKVPairWithTimestamp {
    public let key: String
    public let value: Any?
    public let timestamp: Date
}

enum JSONLDParseError: Error {
    case unexpectedFormat, resourceNotAvailable
}

// MARK: - KBJSONLDGraph
@objc(KBJSONLDGraph)
public class KBJSONLDGraph : NSObject {

    fileprivate let _entities: [KBEntity]
    private let queue: DispatchQueue

    @objc public init(withEntities entities: [KBEntity]?) {
        self._entities = entities ?? []
        self.queue = DispatchQueue(label: "\(KnowledgeBaseBundleIdentifier).KBJSONLDGraph", attributes: .concurrent)
    }

    @objc public var entities: [String] {
        return self._entities.map { $0.identifier }
    }

    public func linkedData(completionHandler: @escaping (Swift.Result<[KBKVPairs], Error>) -> ()) {
        var linkedDataDictionary = [KBKVPairs]()
        let entities = Array(Set(self._entities))
        
        let dispatch = KBTimedDispatch()
        
        self.queue.async {
            for entity in entities {
                dispatch.group.enter()
                KBJSONLDGraph.serialize(entity) { result in
                    switch result {
                    case .failure(let err):
                        log.error("serialization of \(entity) failed. \(err.localizedDescription)")
                        dispatch.interrupt(err)
                    case .success(let object):
                        linkedDataDictionary.append(object)
                        dispatch.group.leave()
                    }
                }
            }
            
            do {
                try dispatch.wait()
                completionHandler(.success(linkedDataDictionary))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
    
    private static func serialize(_ entity: KBEntity, completionHandler: @escaping (Swift.Result<KBKVPairs, Error>) -> ()) {
        var object = KBKVPairs()
        //            object["@context"] = "http://schema.org/"
        object["@id"] = entity.identifier
        
        entity.linkedEntities() { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let predObjTuple):
                for (link, linkedEntity) in predObjTuple {
                    object.append(link, value: linkedEntity.identifier)
                }
                completionHandler(.success(object))
            }
        }
    }
}

extension KBKnowledgeStore {
    
    @objc public func subgraph(withEntities identifiers: [Label]) -> KBJSONLDGraph {
        return KBJSONLDGraph(withEntities: identifiers.map {
            self.entity(withIdentifier: $0)
        })
    }
    
    internal func evaluateJSONLDEntry(forEntity entity: KBEntity,
                                      key: Any,
                                      value: Any,
                                      completionHandler: @escaping KBActionCompletion) {
        guard let _key = key as? String else {
            log.error("key=\(key as! String) is not a string. class = \(String(describing: type(of: key)), privacy: .public)")
            completionHandler(.failure(KBError.unexpectedData(key)))
            return
        }
        
        if _key == "@context" || _key == "@id" {
            completionHandler(.success(()))
            return
        }
        
        if let stringValue = value as? String {
            entity.link(to: self.entity(withIdentifier: stringValue),
                        withPredicate: _key,
                        completionHandler: completionHandler)
        } else if let stringArrayValue = value as? [String] {
            let dispatch = KBTimedDispatch()
            for stringValue in stringArrayValue {
                let otherEntity = self.entity(withIdentifier: stringValue)
                dispatch.group.enter()
                entity.link(to: otherEntity, withPredicate: _key) { result in
                    switch result {
                    case .failure(let err):
                        dispatch.interrupt(err)
                    case .success():
                        dispatch.group.leave()
                    }
                }
            }
            do {
                try dispatch.wait()
                completionHandler(.success(()))
            } catch {
                completionHandler(.failure(error))
            }
        } else if let jsonObject = value as? KBKVPairs {
            let targetEntity = self.entity(withIdentifier: (jsonObject["@id"] ?? "_:\(UUID().uuidString)") as! String)
            
            let dispatch = KBTimedDispatch()
            
            for (k, v) in jsonObject {
                dispatch.group.enter()
                self.evaluateJSONLDEntry(forEntity: targetEntity,
                                         key: k,
                                         value: v) { result in
                    switch result {
                    case .failure(let err):
                        dispatch.interrupt(err)
                    case .success():
                        dispatch.group.leave()
                    }
                }
            }
            
            do {
                try dispatch.wait()
                entity.link(to: targetEntity,
                            withPredicate: _key, completionHandler: completionHandler)
            } catch {
                completionHandler(.failure(error))
            }
        } else if let jsonObjects = value as? [KBKVPairs] {
            let dispatch = KBTimedDispatch()
            for jsonObject in jsonObjects {
                dispatch.group.enter()
                self.evaluateJSONLDEntry(forEntity: entity,
                                         key: key,
                                         value: jsonObject) { result in
                    switch result {
                    case .failure(let err):
                        dispatch.interrupt(err)
                    case .success():
                        dispatch.group.leave()
                    }
                }
            }
            do {
                try dispatch.wait()
                completionHandler(.success(()))
            } catch {
                completionHandler(.failure(error))
            }
        } else {
            completionHandler(.failure(KBError.notSupported))
        }
    }
    
    fileprivate func `import`(entity: KBEntity,
                              fromJsonld jsonObject: Any,
                              completionHandler: KBActionCompletion) {
        if let dictionary = jsonObject as? KBKVPairs {
            let dispatch = KBTimedDispatch()
            for (k, v) in dictionary {
                dispatch.group.enter()
                self.evaluateJSONLDEntry(forEntity: entity,
                                         key: k,
                                         value: v) { result in
                    switch result {
                    case .failure(let err):
                        dispatch.interrupt(err)
                    case .success():
                        dispatch.group.leave()
                    }
                }
            }
            do {
                try dispatch.wait()
                completionHandler(.success(()))
            } catch {
                completionHandler(.failure(error))
            }
        } else {
            completionHandler(.failure(JSONLDParseError.unexpectedFormat))
        }
    }
    
    internal func importJSONLD(data: Data, completionHandler: KBActionCompletion) {
        let evaluate = {
            (object: KBKVPairs, completionHandler: KBActionCompletion) -> Void in
            let entity = self.entity(withIdentifier: (object["@id"] ?? "_:\(UUID().uuidString)") as! String)
            self.import(entity: entity, fromJsonld: object, completionHandler: completionHandler)
        }
        
        let object: Any
        do {
            object = try JSONSerialization.jsonObject(with: data, options: JSONSerialization.ReadingOptions())
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        if let array = object as? [KBKVPairs] {
            let dispatch = KBTimedDispatch()
            for obj in array {
                evaluate(obj) { result in
                    switch result {
                    case .failure(let err):
                        dispatch.interrupt(err)
                    case .success():
                        dispatch.group.leave()
                    }
                }
            }
            do {
                try dispatch.wait()
                completionHandler(.success(()))
            } catch {
                completionHandler(.failure(error))
            }
        } else if let obj = object as? KBKVPairs {
            evaluate(obj) { result in
                switch result {
                case .failure(let err):
                    completionHandler(.failure(err))
                case .success():
                    completionHandler(.success(()))
                }
            }
        } else {
            completionHandler(.failure(KBError.unexpectedData(object)))
        }
    }
    
    public func importContentsOf(JSONLDFileAt path: String, completionHandler: KBActionCompletion) {
        guard FileManager.default.fileExists(atPath: path) else {
            log.error("no such JSONLD file at path \(path, privacy: .public)")
            completionHandler(.failure(JSONLDParseError.resourceNotAvailable))
            return
        }
        
        do {
            let data = try Data(
                contentsOf: URL(fileURLWithPath: path),
                options: Data.ReadingOptions.alwaysMapped
            )
            self.importJSONLD(data: data, completionHandler: completionHandler)
        } catch {
            completionHandler(.failure(error))
        }
    }
}
