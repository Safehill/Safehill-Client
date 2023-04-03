//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/20/21.
//

import Foundation

extension KBEntity {
    /**
     Returns an array of KBEntity objects `self` is connected to.
     Blocking version.
     
     The predicate label needs to match the one passed as argument
     
     - parameter predicate: constraints the search to a specific predicate label
     - parameter matchType: (defaults .equal) defines the match type on the predicate label.
     Note: only .equal and .beginsWith are supported
     - parameter complement: (defaults false) if true returns the complementary set
     
     - returns: the array of KBEntity objects `self` is connected to
     */
    public func linkedEntities(withPredicate predicate: Label,
                               matchType: KBMatchType = .equal,
                               complement wantsComplementarySet: Bool = false) throws -> [(predicate: Label, object: KBEntity)] {
        return try KBSyncMethodReturningInitiable { c in
            self.linkedEntities(withPredicate: predicate,
                                matchType: matchType,
                                complement: wantsComplementarySet,
                                completionHandler: c)
        }
    }
    
    /**
     Returns all KBEntity objects `self` is connected to, and their labeled connections.
     Blocking version
     
     There can be many labeled connections between two entities,
     each having either a different predicate label, or a different target entity (object)
     
     - returns: An array of tuples (predicate: P, object: O)
     */
    public func linkedEntities() throws -> [(predicate: Label, object: KBEntity)] {
        return try KBSyncMethodReturningInitiable(execute: self.linkedEntities)
    }
    
    /**
     Returns an array of KBEntity objects this CKEntity is directly reachable from
     
     The predicate label needs to match exactly the one passed as argument
     
     - parameter predicate: constraints the search to a specific predicate label
     - parameter matchType: (defaults .Equal) defines the match type
     on the predicate label
     - parameter complement: (defaults false) if true returns the complementary set
     
     - returns: The array of CKEntity objects matching the condition
     */
    public func linkingEntities(withPredicate predicate: Label,
                                matchType: KBMatchType = .equal,
                                complement wantsComplementarySet: Bool = false) throws -> [(subject: KBEntity, predicate: Label)] {
        return try KBSyncMethodReturningInitiable { c in
            self.linkingEntities(withPredicate: predicate,
                                 matchType: matchType,
                                 complement: wantsComplementarySet,
                                 completionHandler: c)
        }
    }
    
    /**
     Returns all KBEntity objects this CKEntity is directly reachable from,
     and their labeled connections
     
     There can be many labeled connections between two entities,
     each having either a different predicate label, or a different target entity (object)
     
     - returns: An array of tuples (predicate: P, object: O)
     */
    public func linkingEntities() throws -> [(subject: KBEntity, predicate: Label)] {
        return try KBSyncMethodReturningInitiable(execute: self.linkingEntities)
    }

    /**
     Returns all the predicate labels connecting `this` KBEntity,
     and the one passed as argument
     
     - parameter target: constraints the query to a particular KBEntity
     - parameter matchType: (defaults .Equal) defines the match type
     on the identifier of the linked KBEntity (target)
     
     - returns: The array of predicate labels
     */
    @objc public func links(to target: KBEntity,
                          matchType: KBMatchType = .equal) throws -> [Label] {
        return try KBSyncMethodReturningInitiable { c in
            self.links(to: target,
                       matchType: matchType,
                       completionHandler: c)
        }
    }
}

extension KBEntity {
    /**
     Create a labeled connection between this KBEntity and the one passed as parameter
     
     If the target object has any watcher attached then these will all fire
     
     - parameter target: the KBEntity to connect to
     - parameter predicate: the label on the link
     */
    @objc public func link(to target: KBEntity,
                         withPredicate predicate: Label) throws {
        try KBSyncMethodReturningVoid { c in
            self.link(to: target, withPredicate: predicate, completionHandler: c)
        }
    }
    
    /**
     Remove the link from this KBEntity to the one passed as argument, that matches a certain predicate label
     
     - parameter target: the matching object
     - parameter label: the matching predicate
     - parameter ignoreWeights: if true, removes the links regardless of their weight, otherwise decrements the weight value. Links with weight 0 will be removed
     */
    public func unlink(to target: KBEntity,
                       withPredicate label: Label,
                       ignoreWeights: Bool = false) throws {
        try KBSyncMethodReturningVoid { c in
            self.unlink(to: target,
                        withPredicate: label,
                        ignoreWeights: ignoreWeights,
                        completionHandler: c)
        }
    }
    
    /**
     Remove the entity from the graph
     */
    public func remove() throws {
        try KBSyncMethodReturningVoid { c in
            self.remove(completionHandler: c)
        }
    }

}
