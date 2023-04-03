//
//  CloudKitDataStore.swift
//  
//
//  Created by Gennaro Frazzingaro on 8/1/21.
//

import Foundation

struct KBCloudKitDataStore {
    func mergeRecords<v: NSSecureCoding>(dictionary: [String: v]?,
                                         deletedRecordKeys: [String],
                                         containsAllChanges: Bool) {
        log.debug("")
        let store = KBKVStore.defaultSynchedStore()
        if containsAllChanges {
            // Fetch all the keys in the local SQL database
            store.keys() { result in
                switch result {
                case .success(let keys):
                    var unvisitedKeys: [String] = keys
                    
                    // Save changes to the local SQL database
                    if (dictionary?.count ?? 0 > 0) {
                        let writeBatch = store.writeBatch()
                        writeBatch.set(keysAndValues: dictionary!)
                        writeBatch.write() { _ in }
                        unvisitedKeys.removeAll { s in dictionary!.keys.contains(s) }
                    }
                    
                    // Delete records from the local SQL database
                    if (deletedRecordKeys.count > 0) {
                        store.removeValues(for: deletedRecordKeys) { (_: Swift.Result) in }
                        unvisitedKeys.removeAll { s in deletedRecordKeys.contains(s) }
                    }
                    
                    // Remove any "extra" records from the local SQL database
                    if (unvisitedKeys.count > 0) {
                        store.removeValues(for: unvisitedKeys) { (_: Swift.Result) in }
                    }
                case .failure(let error):
                    log.error("could not retrieve keys: \(error.localizedDescription, privacy: .public)")
                }
            }
        } else {
            if (dictionary?.count ?? 0 > 0) {
                let writeBatch = store.writeBatch()
                writeBatch.set(keysAndValues: dictionary!)
                writeBatch.write() { _ in }
            }
            
            if (deletedRecordKeys.count > 0) {
                store.removeValues(for: deletedRecordKeys) { (_: Swift.Result) in }
            }
        }
        
    }
}
