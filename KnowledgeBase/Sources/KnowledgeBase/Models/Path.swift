//
//  Path.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation

public struct KBPath {
    fileprivate var path: [Label]
    fileprivate var fromSubject: Bool = false
    fileprivate var toObject: Bool = true

    init() {
        self.path = []
    }

    fileprivate init(_ copy: KBPath) {
        self.path = copy.path
        self.fromSubject = copy.fromSubject
        self.toObject = copy.toObject
    }

    public static func from(_ subject: Label) -> KBPath {
        var path = KBPath()
        path.fromSubject = true
        path.path.append(subject)
        return path
    }

    public static func to(_ to: Label, withPredicate predicate: Label) -> KBPath {
        let path = KBPath()
        return path.to(to, withPredicate: predicate)!
    }

    public func to(_ to: Label, withPredicate predicate: Label) -> KBPath? {
        guard self.toObject else { return nil }
        var path = KBPath(self)
        path.path.append(predicate)
        path.path.append(to)
        return path
    }

    public func withPredicate(_ predicate: Label) -> KBPath? {
        guard self.toObject else { return nil }
        var path = KBPath(self)
        path.toObject = false
        path.path.append(predicate)
        return path
    }
}

extension KBSQLHandler {
    
    public func verify(path p: KBPath) throws -> Bool {
        if p.path.count <= 0 { return true }
        else if p.path.count == 1 {
            let condition = KBTripleCondition(subject: p.path[0], predicate: nil, object: nil)
            let matchingTriples = try self.tripleComponents(matching: condition)
            return matchingTriples.count > 0
        } else {
            if let sql = self.connection {
                var query = ""
                var bindings: [Binding?] = []
                var parenthesis = 0
                
                let subject: Label? = p.fromSubject ? p.path[0] : nil
                let start: Int = p.fromSubject ? 3 : 2
                let end: Int = p.toObject ? p.path.count : p.path.count + 1
                
                for index in stride(from: end, through: start, by: -2) {
                    let predicate = p.path[index-2]
                    let object: String? = index > p.path.count ? nil : p.path[index-1]
                    
                    query += "select object from link where predicate = ?"
                    bindings.append(predicate)
                    if let o = object {
                        query += " and object = ?"
                        bindings.append(o)
                    }
                    
                    if index - 2 >= start {
                        query += " and subject in ("
                        parenthesis += 1
                    } else {
                        if let s = subject {
                            query += " and subject = ?"
                            bindings.append(s)
                        }
                        query += String(repeating: ")", count: parenthesis)
                    }
                }
                
                let stmt = try sql.prepare(query, bindings)
                for _ in stmt { return true }
            }
            return false
        }
    }
}
