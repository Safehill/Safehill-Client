//
//  SQLiteHandler.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/22/21.
//

import Foundation
import SQLite3

public let kKBInvalidLinkWeight = -1

enum SQLTableType : String {
    case IntegerValue = "intval", DoubleValue = "realval", StringValue = "textval", AnyValue = "blobval"
    static let allValues = [IntegerValue, DoubleValue, StringValue, AnyValue]
    
    init(value: String) {
        switch(value) {
        case SQLTableType.IntegerValue.rawValue: self = .IntegerValue
        case SQLTableType.DoubleValue.rawValue: self = .DoubleValue
        case SQLTableType.StringValue.rawValue: self = .StringValue
        default: self = .AnyValue
        }
    }
}

public let BlobValueAllowedClasses = [
    NSString.self,
    NSData.self,
    NSArray.self,
    NSDictionary.self,
    NSDate.self,
//    KBHistoricEvent.self,
    KBTriple.self,
    KBPhotoAsset.self
]

@objc(KBSQLHandler)
open class KBSQLHandler: NSObject {

    var connection: Connection?
    
    @objc open class func inMemoryHandler() -> KBSQLHandler? {
        let handler = KBSQLHandler()
        
        guard let connection = KBSQLHandler.createConnection(location: .inMemory) else {
            return nil
        }
        handler.connection = connection
        
        return handler
    }
    
    private override init() {
        self.connection = nil
        super.init()
    }
    
    @objc public init?(name: String, baseURL: URL) {
        let allowedCharSet = NSMutableCharacterSet()
        allowedCharSet.formUnion(with: CharacterSet.alphanumerics)
        allowedCharSet.addCharacters(in: "_-")
        allowedCharSet.addCharacters(in: ".")
        
        let safeName = name.components(separatedBy: allowedCharSet.inverted).joined()
        if safeName.isEmpty {
            log.fault("invalid database name \(name, privacy: .public)")
            return nil
        }
        
        do {
            try KBSQLHandler.createDirectory(at: baseURL.path)
            log.debug("using directory at path \(baseURL.path, privacy: .public)")
        }
        catch {
            log.fault("could not create database directory: \(error.localizedDescription, privacy: .public)")
            return nil
        }
        
        let dbURL = baseURL
            .appendingPathComponent(safeName)
            .appendingPathExtension(DatabaseExtension)
        
        guard let connection = KBSQLHandler.createConnection(location: .uri(dbURL.path)) else {
            log.fault("could not create connection to the database")
            return nil
        }
        self.connection = connection
        
        super.init()
    }
    
    /// Creates the database directory if it does not already exist.
    /// On embedded OSes, it converts the directory into a data
    /// vault.
    ///
    /// macOS does not use a data vault. As a multi-user OS, the database
    /// directory sits inside the user directory, and if it were converted
    /// to a data vault it would prevent other system agents from deleting
    /// the user directory.
    private static func createDirectory(at path: String) throws {
        try KBDataVault.createDirectory(at: path)
    }
    
    private static func createConnection(location: Connection.Location) -> Connection? {
        var connection: Connection? = nil
        
        do {
            connection = try Connection(location)
            try connection?.execute(kKBTypedKeyValuePairsDbSchema)
        } catch let error as Result {
            if (error as NSError).code != SQLITE_OK {
                log.error("error creating SQL schema. \(error.localizedDescription, privacy: .public)")
            }
        } catch {
            log.error("couldn't establish a connection to the database. \(error.localizedDescription, privacy: .public)")
        }
        
        return connection
    }

    //MARK: - KVS
    
    @objc open func keys() throws -> [String] {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var keys = [String]()
        
        let query = SQLTableType.allValues.map { "select k from \($0.rawValue)" }.joined(separator: " union all ")
        let stmt = try connection.prepare(query)
        for row in stmt {
            assert(row.count == 1, "retrieved the right number of columns")
            keys.append(row[0] as! String)
        }
        
        return keys
    }
    
    @objc open func keys(matching condition: KBGenericCondition) throws -> [String] {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var keys = [String]()
        
        let query = SQLTableType.allValues.map { "select k from \($0.rawValue) where \(condition.sql)" }.joined(separator: " union all ")
        
        let stmt = try connection.prepare(query)
        for row in stmt {
            assert(row.count == 1, "retrieved the right number of columns")
            keys.append(row[0] as! String)
        }
        
        return keys
    }
    
    @objc open func values() throws -> [Any] {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var values = [Any]()
        
        let query = SQLTableType.allValues.map { "select v from \($0.rawValue)" }.joined(separator: " union all ")
        let stmt = try connection.prepare(query)
        for row in stmt {
            assert(row.count == 1, "retrieved the right number of columns")
            if let value = try self.deserializeValue(row[0]) {
                values.append(value)
            }
        }
        
        return values
    }
    
    @objc open func values(forKeysMatching condition: KBGenericCondition) throws -> [Any] {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var values = [Any]()
        
        let query = SQLTableType.allValues.map { "select v from \($0.rawValue) where \(condition.sql)" }.joined(separator: " union all ")
        let stmt = try connection.prepare(query)
        for row in stmt {
            assert(row.count == 1, "retrieved the right number of columns")
            if let value = try self.deserializeValue(row[0]) {
                values.append(value)
            }
        }
        
        return values
    }
    
    @objc open func keysAndValues() throws -> KBKVPairs {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var dict = KBKVPairs()
        
        let query = SQLTableType.allValues.map { "select k, v from \($0.rawValue)" }.joined(separator: " union all ")
        let stmt = try connection.prepare(query)
        for row in stmt {
            assert(row.count == 2, "retrieved the right number of columns")
            if let key = try self.deserializeValue(row[0]) as? String,
                let value = try self.deserializeValue(row[1]){
                dict[key] = value
            }
        }
        
        return dict
    }
    
    @objc open func keysAndValues(within interval: DateInterval, limit: Int, order: ComparisonResult) throws -> [Date: KBKVPairs] {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var pairsByDate = [Date: KBKVPairs]()
        let sqlOrder: String = order == .orderedAscending ? "asc" : "desc"
        
        let query = SQLTableType.allValues.map { "select k, v, t from \($0.rawValue) where t between ? and ?" }
            .joined(separator: " union all ") + " order by t \(sqlOrder) limit ?"
        let dateBindings: [Binding?] = [interval.start.timeIntervalSince1970, interval.end.timeIntervalSince1970]
        var bindings: [Binding?] = ([[Binding?]](repeating: dateBindings, count: SQLTableType.allValues.count )).flatMap { $0 }
        bindings.append(limit)
        
        let stmt = try connection.prepare(query, bindings)
        for row in stmt {
            var dict = KBKVPairs()
            assert(row.count == 3, "retrieved the right number of columns")
            if let key = try self.deserializeValue(row[0]) as? String,
               let value = try self.deserializeValue(row[1]),
               let timestamp = row[2] as? TimeInterval
            {
                dict[key] = value
                pairsByDate[Date(timeIntervalSince1970: timestamp)] = dict
            }
        }
        
        return pairsByDate
    }
    
    @objc open func keysAndvalues(forKeysMatching condition: KBGenericCondition) throws -> KBKVPairs {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var dict = KBKVPairs()
        
        let query = SQLTableType.allValues.map { "select k, v from \($0.rawValue) where \(condition.sql)" }.joined(separator: " union all ")
        let stmt = try connection.prepare(query)
        for row in stmt {
            assert(row.count == 2, "retrieved the right number of columns")
            if let key = try self.deserializeValue(row[0]) as? String,
                let value = try self.deserializeValue(row[1]){
                dict[key] = value
            }
        }
        
        return dict
    }
    
    internal func selectQuery(project: [String],
                              whereField: String,
                              isIn array: [Binding?]) -> (String, [Binding?]) {
        let arrayQuery = [String](repeating: "?", count: array.count).joined(separator: ",")
        let projection = project.joined(separator: ",")
        let query = SQLTableType.allValues
            .map { "select \(projection) from \($0.rawValue) where \(whereField) in (\(arrayQuery))" }
            .joined(separator: " union all ")
        let bindings: [Binding?] = ([[Binding?]](repeating: array, count: SQLTableType.allValues.count )).flatMap { $0 }
        
        return (query, bindings)
    }
    
    @objc open func values(for keys: [String]) throws -> [Any] {
        let (query, bindings) = self.selectQuery(project: ["k", "v"],
                                                 whereField: "k",
                                                 isIn: keys.map { $0 as Binding? })
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var valuesByKey = [String: Any]()
        var values = [Any]()
        
        let stmt = try connection.prepare(query, bindings)
        for row in stmt {
            assert(row.count == 2, "retrieved the right number of columns")
            if let key = row[0] as? String {
                if let value = try self.deserializeValue(row[1]) {
                    valuesByKey.append(key, value: value)
                } else {
                    valuesByKey.append(key, value: NSNull())
                }
            }
        }
        
        for key in keys {
            if let value = valuesByKey[key] {
                values.append(value)
            } else {
                values.append(NSNull())
            }
        }
        
        return values
    }
    
    @objc(saveKeysAndValues:error:)
    open func save(keysAndValues: KBKVPairs) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var keysToRemove = [String]()
        
        try connection.transaction(Connection.TransactionMode.immediate) {
            for (key, value) in keysAndValues {
                
                let format = "insert or replace into %@ (k, v, t) values (?, ?, ?)"
                let query: String
                var bindings = Array<Binding?>()
                let timestamp = Date().timeIntervalSince1970
                
                switch(value) {
                case is NSNull:
                    keysToRemove.append(key)
                    continue
                case let doubleValue as Double:
                    query = String(format: format, SQLTableType.DoubleValue.rawValue)
                    bindings = [key, doubleValue, timestamp]
                    break
                case let numberValue as Number:
                    guard let nsnumber = numberValue as? NSNumber else {
                        throw KBError.fatalError("Could not convert Number to NSNumber for value: \(value)")
                    }
                    guard let int64Value = Int64(exactly: nsnumber) else {
                        throw KBError.fatalError("Could not convert Numeric to Int64 for value: \(value)")
                    }
                    query = String(format: format, SQLTableType.IntegerValue.rawValue)
                    bindings = [key, int64Value, timestamp]
                    break
                case let intValue as Int:
                    query = String(format: format, SQLTableType.IntegerValue.rawValue)
                    bindings = [key, intValue, timestamp]
                    break
                case let boolValue as Bool:
                    query = String(format: format, SQLTableType.IntegerValue.rawValue)
                    bindings = [key, boolValue ? 1 : 0, timestamp]
                    break
                case let stringValue as String:
                    query = String(format: format, SQLTableType.StringValue.rawValue)
                    bindings = [key, stringValue, timestamp]
                    break
                default:
                    let data: Data
                    if #available(macOS 10.13, *) {
                        data = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
                    } else {
                        data = NSKeyedArchiver.archivedData(withRootObject: value)
                    }
                    query = String(format: format, SQLTableType.AnyValue.rawValue)
                    bindings = [key, data.datatypeValue, timestamp]
                }
                
                try connection.run(query, bindings)
            }
            
            if keysToRemove.count > 0 {
                try self._removeValues(for: keysToRemove)
            }
        }
    }
    
    @objc open func removeValue(for key: String) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        try connection.transaction(Connection.TransactionMode.immediate) {
            for query in SQLTableType.allValues.map({ "delete from \($0.rawValue) where k = ?" } ){
                _ = try connection.run(query, key)
            }
        }
    }
    
    fileprivate func _removeValues(for keys: [String]) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        let keysArrayQuery = [String](repeating: "?", count: keys.count).joined(separator: ",")
        let keysArrayValues = keys.map { $0 as Binding? }
        
        for query in SQLTableType.allValues.map({ "delete from \($0.rawValue) where k in (\(keysArrayQuery))" } ){
            _ = try connection.run(query, keysArrayValues)
        }
    }
    
    @objc open func removeValues(for keys: [String]) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        try connection.transaction(Connection.TransactionMode.immediate) {
            try self._removeValues(for: keys)
        }
    }
    
    fileprivate func _removeValues(forKeysMatching condition: KBGenericCondition) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        _ = try connection.run("pragma case_sensitive_like = true")
        for query in SQLTableType.allValues.map({ "delete from \($0.rawValue) where \(condition.sql)" }) {
            _ = try connection.run(query)
        }
    }
    
    @objc open func removeValues(forKeysMatching condition: KBGenericCondition) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        try connection.transaction(Connection.TransactionMode.immediate) {
            try self._removeValues(forKeysMatching: condition)
        }
    }
    
    //MARK: - Triple retrieval
    
    @objc open func tripleComponents(matching condition: KBTripleCondition?) throws -> [KBTriple] {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var triples = Set<KBTriple>()
        
        _ = try connection.run("pragma case_sensitive_like = true")
        
        var query = "select k, v from blobval"
        if let c = condition {
            query += " where \(c.rawCondition.sql)"
        }
        
        let stmt = try connection.prepare(query)
        for row in stmt {
            assert(row.count == 2, "retrieved the right number of columns")
            
            if let blob = row[1] as? Blob {
                let data = Data.fromDatatypeValue(blob)
                let unarchiver: NSKeyedUnarchiver
                if #available(macOS 10.13, *) {
                    unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
                } else {
                    unarchiver = NSKeyedUnarchiver(forReadingWith: data)
                }
                if let triple = unarchiver.decodeObject(of: KBTriple.self, forKey: NSKeyedArchiveRootObjectKey) {
                    triples.insert(triple)
                }
                unarchiver.finishDecoding()
            } else {
                throw KBError.unexpectedData(row[1])
            }
        }
        return Array(triples)
    }
    
    //MARK: - Graph Links
    
    @objc open func setWeight(forLinkWithLabel predicate: Label,
                              between subjectIdentifier: Label,
                              and objectIdentifier: Label,
                              toValue value: Int) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        let linkID = KBSQLHandler.linkIdentifier(forLinkWithLabel: predicate,
                                                             between: subjectIdentifier,
                                                             and: objectIdentifier)
        
        let sql = "insert or replace into link (id, subject, predicate, object, count) values (?, ?, ?, ?, ?)"
        let sqlBindings: [Binding?] = [linkID, subjectIdentifier, predicate, objectIdentifier, value]
        
        try connection.transaction(Connection.TransactionMode.immediate) {
            _ = try connection.run(sql, sqlBindings)
        }
        
        // reflect the change in the Hexastore
        var kvs = KBKVPairs()
        let tripleValue = KBTriple(subject: subjectIdentifier,
                                   predicate: predicate,
                                   object: objectIdentifier,
                                   weight: value)
        for hexatype in KBHexastore.allValues {
            let key = hexatype.hexaValue(subject: subjectIdentifier,
                                         predicate: predicate,
                                         object: objectIdentifier)
            kvs[key] = tripleValue
        }
        try self.save(keysAndValues: kvs)
    }
    
    // Objective-C counterpart of Swift method below
    @objc open func increaseLinkWeight(forLinkWithLabel predicate: Label,
                                       between subjectIdentifier: Label,
                                       and objectIdentifier: Label) -> Int {
        do {
            return try self.increaseWeight(forLinkWithLabel: predicate,
                                           between: subjectIdentifier,
                                           and: objectIdentifier)
        } catch {
            log.error("error: \(error.localizedDescription, privacy: .public)")
            return kKBInvalidLinkWeight
        }
    }
    
    open func increaseWeight(forLinkWithLabel predicate: Label,
                            between subjectIdentifier: Label,
                            and objectIdentifier: Label) throws -> Int {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        let linkID = KBSQLHandler.linkIdentifier(forLinkWithLabel: predicate,
                                                             between: subjectIdentifier,
                                                             and: objectIdentifier)
        let sql = "insert or replace into link "
            + "(id, subject, predicate, object, count)"
            + " values "
            + "(?, ?, ?, ?, coalesce((select count from link where id = ?), 0) + 1)"
        let sqlBindings : [Binding?] = [linkID, subjectIdentifier, predicate, objectIdentifier, linkID]
        
        var weights = [Int]()
        
        try connection.transaction(Connection.TransactionMode.immediate) {
            _ = try connection.run(sql, sqlBindings)
        }
        
        let query = "select count from link where id = ?";
        let queryBindings = [linkID];
        
        for row in try connection.prepare(query, queryBindings) {
            assert(row.count == 1, "retrieved the right number of columns")
            assert(row[0] as? Int64 != nil, "types match")
            
            weights.append(Int(truncatingIfNeeded: row[0] as! Int64))
        }
        
        assert(weights.count == 1, "one entry per triple")
        
        // reflect the change in the Hexastore
        var kvs = KBKVPairs()
        let tripleValue = KBTriple(subject: subjectIdentifier,
                                   predicate: predicate,
                                   object: objectIdentifier,
                                   weight: weights[0])
        for hexatype in KBHexastore.allValues {
            let key = hexatype.hexaValue(subject: subjectIdentifier,
                                         predicate: predicate,
                                         object: objectIdentifier)
            kvs[key] = tripleValue
        }
        try self.save(keysAndValues: kvs)
        
        return weights[0]
    }
    
    // Objective-C counterpart of Swift method below
    @objc open func decreaseLinkWeight(forLinkWithLabel predicate: Label,
                                       between subjectIdentifier: Label,
                                       and objectIdentifier: Label) -> Int {
        do {
            return try self.decreaseWeight(forLinkWithLabel: predicate,
                                           between: subjectIdentifier,
                                           and: objectIdentifier)
        } catch {
            log.error("error: \(error.localizedDescription, privacy: .public)")
            return kKBInvalidLinkWeight
        }
    }
    
    open func decreaseWeight(forLinkWithLabel predicate: Label,
                            between subjectIdentifier: Label,
                            and objectIdentifier: Label) throws -> Int {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var weights = [Int]()
        
        let (whereClause, bindings) = KBSQLHandler.whereClause(forLinkWithLabel: predicate,
                                                                           between: subjectIdentifier,
                                                                           and: objectIdentifier)
        let query = "select id, count, predicate, object from link where \(whereClause)";
        
        for row in try connection.prepare(query, bindings) {
            assert(row.count == 4, "retrieved the right number of columns")
            assert(row[0] as? String != nil
                && row[1] as? Int64 != nil
                && row[2] as? String != nil
                && row[3] as? String != nil,
                   "types match"
            )
            
            let expectedLinkId = KBSQLHandler.linkIdentifier(forLinkWithLabel: predicate,
                                                                         between: subjectIdentifier,
                                                                         and: objectIdentifier)
            assert(row[0] as! String == expectedLinkId)
            
            let pred = row[2] as! String
            
            if let count = row[1] as? Int64 , count > 1 {
                let newValue = Int(count - 1)
                try self.setWeight(forLinkWithLabel: pred,
                                   between: subjectIdentifier,
                                   and: objectIdentifier,
                                   toValue: newValue)
                weights.append(newValue)
            } else {
                let object = row[3] as! String
                try self.dropLink(withLabel: pred,
                                  between: subjectIdentifier,
                                  and: object)
                
                weights.append(0)
            }
        }
        
        assert(weights.count == 1, "one entry per triple")
        return weights[0]
    }
    
    internal func decreaseWeights(between subjectIdentifier: Label,
                                  and objectIdentifier: Label) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        let (whereClause, bindings) = KBSQLHandler.whereClause(forLinkWithLabel: nil,
                                                                           between: subjectIdentifier,
                                                                           and: objectIdentifier)
        let query = "select count, predicate, object from link where \(whereClause)";
        
        for row in try connection.prepare(query, bindings) {
            assert(row.count == 3, "retrieved the right number of columns")
            assert(row[0] as? Int64 != nil
                && row[1] as? String != nil
                && row[2] as? String != nil,
                   "types match"
            )
            
            let pred = row[1] as! String
            
            if let count = row[1] as? Int64 , count > 1 {
                try self.setWeight(forLinkWithLabel: pred,
                                   between: subjectIdentifier,
                                   and: objectIdentifier,
                                   toValue: Int(count) - 1)
            } else {
                let object = row[3] as! String
                try self.dropLink(withLabel: pred,
                                  between: subjectIdentifier,
                                  and: object)
            }
        }
    }
    
    @objc open func dropLink(withLabel predicate: Label,
                             between subjectIdentifier: Label,
                             and objectIdentifier: Label) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        let sql = "delete from link where id = ?"
        let linkID = KBSQLHandler.linkIdentifier(forLinkWithLabel: predicate,
                                                             between: subjectIdentifier,
                                                             and: objectIdentifier)
        try connection.transaction(Connection.TransactionMode.immediate) {
            _ = try connection.run(sql, [linkID])
        }

        // reflect the change in the Hexastore
        let condition = KBTripleCondition(
            subject: subjectIdentifier,
            predicate: predicate,
            object: objectIdentifier
        )
        try self.removeValues(forKeysMatching: condition.rawCondition)
    }

    @objc open func dropLinks(withLabel predicate: String?,
                              from subjectIdentifier: String) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        var sqlBindings = [Binding?]()
        let whereClause: String
        if let _ = predicate {
            whereClause = "id like ?"
            let value = KBHexastore.JOINER.combine(
                KBHexastore.PSO.rawValue,
                subjectIdentifier,
                predicate!
            )
            sqlBindings.append(value + "%")
        } else {
            whereClause = "subject = ?";
            sqlBindings.append(subjectIdentifier)
        }
        let sql = "delete from link where \(whereClause)"
        
        try connection.transaction(Connection.TransactionMode.immediate) {
            _ = try connection.run(sql, sqlBindings)
        }
        
        // reflect the change in the Hexastore
        let condition = KBTripleCondition(
            subject: subjectIdentifier,
            predicate: predicate,
            object: nil
        )
        try self._removeValues(forKeysMatching: condition.rawCondition)
    }
    
    @objc(dropLinksBetween:and:error:)
    open func dropLinks(between subjectIdentifier: String,
                        and objectIdentifier: String) throws {
        guard let connection = self.connection else {
            throw KBError.databaseNotReady
        }
        
        let (whereClause, bindings) = KBSQLHandler.whereClause(forLinkWithLabel: nil,
                                                                           between: subjectIdentifier,
                                                                           and: objectIdentifier)

        let sql = "delete from link where \(whereClause)"
        try connection.transaction(Connection.TransactionMode.immediate) {
            _ = try connection.run(sql, bindings)
        }
        
        // reflect the change in the Hexastore
        let condition = KBTripleCondition(
            subject: subjectIdentifier,
            predicate: nil,
            object: objectIdentifier
        )
        try self.removeValues(forKeysMatching: condition.rawCondition)
    }
    
    //MARK: - Utils
    
    fileprivate static func whereClause(forLinkWithLabel predicate: Label?,
                                        between subjectIdentifier: Label,
                                        and objectIdentifier: Label) -> (String, [Binding?]) {
        let whereClause: String
        var sqlBindings = [Binding?]()
        if let _ = predicate {
            whereClause = "id = ?"
            let linkID = KBSQLHandler.linkIdentifier(forLinkWithLabel: predicate!,
                                                                 between: subjectIdentifier,
                                                                 and: objectIdentifier)
            sqlBindings.append("\(linkID)")
        } else {
            whereClause = "subject = ? and object = ?";
            sqlBindings = [subjectIdentifier, objectIdentifier]
        }
        
        return (whereClause, sqlBindings)
    }
    
    fileprivate static func linkIdentifier(forLinkWithLabel predicate: Label,
                                           between subjectIdentifier: Label,
                                           and objectIdentifier: Label) -> String {
        return KBHexastore.PSO.hexaValue(
            subject: subjectIdentifier,
            predicate: predicate,
            object: objectIdentifier
        )
    }
    
    fileprivate func deserializeValue(_ value: Binding?) throws -> Any? {
        switch(value) {
        case let doubleValue as Double:
            return doubleValue
        case let numberValue as Number:
            guard numberValue is Int64 else {
                throw KBError.fatalError("Could not convert Numeric to Int64")
            }
            return NSNumber(value: numberValue as! Int64)
        case let stringValue as String:
            return stringValue
        case let blob as Blob:
            let data = Data.fromDatatypeValue(blob)
            let unarchiver: NSKeyedUnarchiver
            if #available(macOS 10.13, *) {
                unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
            } else {
                unarchiver = NSKeyedUnarchiver(forReadingWith: data)
            }
            let unarchived = unarchiver.decodeObject(of: BlobValueAllowedClasses, forKey: NSKeyedArchiveRootObjectKey)
            unarchiver.finishDecoding()
            return unarchived
        default:
            throw KBError.unexpectedData(value)
        }
    }
}
