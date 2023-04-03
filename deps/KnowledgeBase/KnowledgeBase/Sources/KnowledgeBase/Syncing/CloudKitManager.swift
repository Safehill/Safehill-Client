//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/30/21.
//

import Foundation
import CloudKit
import CoreFoundation

typealias KBCloudKitManagerFetchRecordsPrivateCompletionBlock = (Error?, [CKRecord]?, [CKRecord.ID]?, Bool, CKServerChangeToken?) -> ()

public typealias KBCloudKitManagerFetchRecordsCompletionBlock = (Error?, [CKRecord]?, [CKRecord.ID]?, Bool) -> ()
public typealias KBCloudKitManagerModifyRecordsCompletionBlock = (Error?, [CKRecord]?, [CKRecord.ID]?) -> ()

let kKBKnowledgeBaseCloudKitContainerIdentifier = "com.gf.knowledgebase"
let kKBCloudKitRecordZoneName = "com.gf.knowledgebase"
let kKBCloudKitZoneServerChangeToken = "ServerChangeToken"
let kKBCloudKitHasSetUpRecordZoneSubscription = "HasSetUpRecordZoneSubscription"
let kKBCloudKitZoneSubscriptionPrefix = "com.gf.knowledgebase.subscription"

let kKBCloudKitKeyValueRecordType = "com.gf.knowledgebase.KeyValueRecord"
let kKBCloudKitKeyValueRecordTypeVersion = 1
let kKBCloudKitKeyValueRecordTypeVersionKey = "version"
let kKBCloudKitKeyValueRecordKeyKey = "key"

let kKBCloudKitSetupRetryIntervalMax: TimeInterval = 24 * 60 * 60.0     // One day
let kKBCloudKitSetupRetryMultiplier = 2.0                               // 2x


extension CKError {
    func containsZoneNotFound() -> Bool {
        var val = false
        switch self {
        case CKError.zoneNotFound:
            val = true
        case CKError.partialFailure:
            if let partialErrors = self.userInfo[CKPartialErrorsByItemIDKey] as? [AnyHashable: CKError] {
                val = partialErrors.values.contains { $0.code == CKError.zoneNotFound }
            }
        default: break
        }
        return val
    }
}


@objc(KBCloudKitManager)
public class KBCloudKitManager : NSObject {
    
    public static let shared = KBCloudKitManager()
    static let recordZoneID = CKRecordZone.ID(zoneName: kKBCloudKitRecordZoneName, ownerName: CKCurrentUserDefaultName)
    
    public var accountStatus: CKAccountStatus = .couldNotDetermine
    
    private var container = CKContainer(identifier: kKBKnowledgeBaseCloudKitContainerIdentifier)
    private var recordZone: CKRecordZone? = nil
    private var subscription: CKRecordZoneSubscription? = nil
    
    private var dataStore = KBCloudKitDataStore()
    
    private let serialQueue: DispatchQueue
    
    private var recordZoneSetupTimer: DispatchSource? = nil
    private var subscriptionSetupTimer: DispatchSource? = nil
    
    private var hasSetUpRecordZoneSubscription: Bool {
        get {
            do {
                let store = KBKVStore.userDefaultsStore()
                if let number = try store.value(for: kKBCloudKitHasSetUpRecordZoneSubscription) as? NSNumber {
                    return number.boolValue
                }
            } catch {
                log.info("Could not retrieve \(kKBCloudKitHasSetUpRecordZoneSubscription, privacy: .public) from local cache: \(error.localizedDescription, privacy: .public)")
            }
            return false
        }
        set(value) {
            do {
                let store = KBKVStore.userDefaultsStore()
                try store.set(value: NSNumber(booleanLiteral: value),
                              for: kKBCloudKitHasSetUpRecordZoneSubscription)
                
            } catch {
                log.error("Could not save \(kKBCloudKitHasSetUpRecordZoneSubscription, privacy: .public) to local cache: \(error.localizedDescription, privacy: .public)")
            }
        }
    }
    
    private var serverChangeToken: CKServerChangeToken? {
        get {
            do {
                let store = KBKVStore.userDefaultsStore()
                if let data = try store.value(for: kKBCloudKitZoneServerChangeToken) as? Data {
                    let unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
                    
                    let token = unarchiver.decodeObject(of: CKServerChangeToken.self,
                                                        forKey: NSKeyedArchiveRootObjectKey)
                    unarchiver.finishDecoding()
                    return token
                }
            } catch {
                log.error("could not retrieve cloudkit settings from user defaults store: \(error.localizedDescription, privacy: .public)")
            }
            return nil
        }
        set(value) {
            do {
                let store = KBKVStore.userDefaultsStore()
                if let value = value {
                    let archivedToken = try NSKeyedArchiver.archivedData(withRootObject: value,
                                                                         requiringSecureCoding: true)
                    try store.set(value: archivedToken,
                                  for: kKBCloudKitHasSetUpRecordZoneSubscription)
                } else {
                    try store.set(value: nil,
                                  for: kKBCloudKitHasSetUpRecordZoneSubscription)
                }
            } catch {
                log.error("Could not save \(kKBCloudKitHasSetUpRecordZoneSubscription, privacy: .public) to local cache: \(error.localizedDescription, privacy: .public)")
            }
        }
    }
    
    private override init() {
        self.serialQueue = DispatchQueue(label: "KBCloudKitManager.Serial")
        super.init()
        
        self.serialQueue.async {
            // Initialize Zone
            self.recordZone = CKRecordZone(zoneID: KBCloudKitManager.recordZoneID)
            
            // Add Subscription
            let subscriptionID = "\(kKBCloudKitZoneSubscriptionPrefix)-1"
            self.subscription = CKRecordZoneSubscription(zoneID: KBCloudKitManager.recordZoneID,
                                                         subscriptionID: subscriptionID)
            
            self.updateAccountStatus()
        }
        
        NotificationCenter.default.addObserver(self,
                                               selector: #selector(cloudKitAccountDidChange(_:)),
                                               name: .CKAccountChanged,
                                               object: nil)
    }
    
    deinit {
        NotificationCenter.default.removeObserver(self, name: .CKAccountChanged, object: nil)
    }
    
    // MARK: CKAccountStatus
    
    private func updateAccountStatus() {
        Dispatch.dispatchPrecondition(condition: .onQueue(self.serialQueue))
        
        self.container.accountStatus() { [unowned self] (accountStatus, error) in
            if let error = error { log.error("\(error.localizedDescription, privacy: .public)") }

            // Update Account Status
            self.accountStatus = accountStatus
            self.setupAccountState()
        }
    }
    
    private func setupAccountState() {
        self.cancel(timer: &self.recordZoneSetupTimer)
        self.cancel(timer: &self.subscriptionSetupTimer)
        if self.accountStatus == CKAccountStatus.available {
            self.setupRecordZone()
        }
    }
    
    // MARK: CKRecordZone
    
    private func createRecordZone(completionHandler: @escaping (Swift.Result<CKRecordZone?, Error>) -> ()) {
        Dispatch.dispatchPrecondition(condition: .onQueue(self.serialQueue))
        log.trace("Creating zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public))")
        
        let recordZone = CKRecordZone(zoneID: KBCloudKitManager.recordZoneID)
        let modifyRecordZonesOperation = CKModifyRecordZonesOperation(recordZonesToSave: [recordZone],
                                                                      recordZoneIDsToDelete: nil)
        modifyRecordZonesOperation.modifyRecordZonesCompletionBlock = {
            (savedRecordZones, deletedRecordZoneIDs, operationError) in
            if let error = operationError {
                log.error("Failed to create zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)): \(error.localizedDescription, privacy: .public)")
                completionHandler(.failure(error))
            } else {
                completionHandler(.success((savedRecordZones?.first)))
            }
        }
        modifyRecordZonesOperation.qualityOfService = .utility
        self.container.privateCloudDatabase.add(modifyRecordZonesOperation)
    }
    
    private func setupRecordZone(withRetryInterval interval: TimeInterval = kKBCloudKitSetupRetryIntervalMax) {
        Dispatch.dispatchPrecondition(condition: .onQueue(self.serialQueue))
        
        self.createRecordZone { result in
            self.serialQueue.async {
                self.cancel(timer: &self.recordZoneSetupTimer)
                
                let retryBlock = { (error: Error?) in
                    // Increase the retry interval by the multiplier on each failure
                    let nextRetryInterval = min(kKBCloudKitSetupRetryIntervalMax, interval * kKBCloudKitSetupRetryMultiplier)
                    KBScheduleRoutine(withRetryInterval: interval, onQueue: self.serialQueue) { [weak self] in
                        self?.setupRecordZone(withRetryInterval: nextRetryInterval)
                    }
                    log.error("Failed to create zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)): \(error?.localizedDescription ?? "<nil>", privacy: .public). Retrying in \(nextRetryInterval) seconds")
                }
                
                switch result {
                case .failure(let error):
                    retryBlock(error)
                case .success(let recordZone):
                    if let recordZone = recordZone {
                        log.info("Zone created (\(recordZone.zoneID.zoneName, privacy: .public)")
                        
                        if recordZone.isEqual(self.recordZone) == false {
                            self.recordZone = recordZone
                            self.serverChangeToken = nil
                        }
                        
                        if self.subscription == nil || self.hasSetUpRecordZoneSubscription {
                            self.setUpRecordZoneSubscription()
                        } else {
                            self.fetchChanges()
                        }
                    } else {
                        retryBlock(KBError.fatalError("No error and no recordZone returned from createRecordZone"))
                    }
                }
            }
        }
    }
    
    private func deleteRecordZone(completionHandler: @escaping KBActionCompletion) {
        Dispatch.dispatchPrecondition(condition: .onQueue(self.serialQueue))
        log.trace("Deleting zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)")
        let modifyRecordZonesOperation = CKModifyRecordZonesOperation(recordZonesToSave: nil,
                                                                      recordZoneIDsToDelete: [KBCloudKitManager.recordZoneID])
        modifyRecordZonesOperation.modifyRecordZonesCompletionBlock = {
            (savedRecordZones, deletedRecordZoneIDs, error) in
            if let error = error {
                log.error("Failed to delete zone \(KBCloudKitManager.recordZoneID.zoneName, privacy: .public): \(error.localizedDescription, privacy: .public)")
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
        modifyRecordZonesOperation.qualityOfService = .utility
        self.container.privateCloudDatabase.add(modifyRecordZonesOperation)
    }
    
    // MARK: CKRecordZoneSubscription
    
    private func setUpRecordZoneSubscription(withRetryInterval interval: TimeInterval = kKBCloudKitSetupRetryIntervalMax) {
        Dispatch.dispatchPrecondition(condition: .onQueue(self.serialQueue))
        log.trace("Creating record zone subscription (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)")
        
        let subscriptionID = "\(kKBCloudKitZoneSubscriptionPrefix)-1"
        let subscription = CKRecordZoneSubscription(zoneID: KBCloudKitManager.recordZoneID,
                                                    subscriptionID: subscriptionID)
        
        // Set the "shouldSendContentAvailable" to increase the priority
        // See: <https://developer.apple.com/reference/cloudkit/cknotificationinfo?language=objc>
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        
        let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription],
                                                       subscriptionIDsToDelete: nil)
        operation.qualityOfService = .utility
        operation.modifySubscriptionsCompletionBlock = {
            savedSubscriptions, deletedSubscriptionIDs, error in
            self.serialQueue.async {
                self.cancel(timer: &self.subscriptionSetupTimer)
                
                if let subscriptions = savedSubscriptions, subscriptions.contains(subscription) {
                    log.info("Subscription created: (\(String(describing: self.subscription?.subscriptionID), privacy: .public)")
                    self.subscription = subscription
                    self.hasSetUpRecordZoneSubscription = true
                    self.fetchChanges()
                } else if let operationError = error as? CKError, operationError.containsZoneNotFound() {
                    if self.recordZone != nil {
                        self.cleanup()
                        self.setupRecordZone()
                    } else {
                        self.hasSetUpRecordZoneSubscription = false
                    }
                } else {
                    // Increase the retry interval by the multiplier on each failure
                    let nextRetryInterval = min(kKBCloudKitSetupRetryIntervalMax, interval * kKBCloudKitSetupRetryMultiplier)
                    KBScheduleRoutine(withRetryInterval: interval, onQueue: self.serialQueue) { [weak self] in
                        self?.setUpRecordZoneSubscription(withRetryInterval: nextRetryInterval)
                    }
                    log.info("Subscription creation failed: (\(error?.localizedDescription ?? "<nil>", privacy: .public)). Retrying in \(nextRetryInterval, privacy: .public) seconds")
                    return
                }
            }
        }
        
        self.container.privateCloudDatabase.add(operation)
    }
    
    private func cancel(timer: inout DispatchSource?) {
        Dispatch.dispatchPrecondition(condition: .onQueue(self.serialQueue))
        
        if timer != nil {
            timer!.cancel()
            timer = nil
        }
    }
    
    internal func cleanup() {
        self.cancel(timer: &self.recordZoneSetupTimer)
        self.cancel(timer: &self.subscriptionSetupTimer)
        
        self.recordZone = nil
        self.subscription = nil
        self.hasSetUpRecordZoneSubscription = false
        self.serverChangeToken = nil
    }
    
    
    // MARK: - Notification Handlers

    @objc func cloudKitAccountDidChange(_ notification: NSNotification) {
        log.info("")
        self.serialQueue.async {
            self.updateAccountStatus()
        }
    }
    
    // MARK: - Fetching
    
    private func fetchChanges(withRetryCount count: Int = 0) {
        Dispatch.dispatchPrecondition(condition: .onQueue(self.serialQueue))
        log.debug("Fetching changes (retry=\(count, privacy: .public)")
        
        self.fetchChanges(from: self.serverChangeToken ?? nil) {
            error, changedRecords, deletedRecords, isAll, newToken in
            self.serialQueue.async {
                guard error == nil else {
                    switch error! {
                    case CKError.changeTokenExpired:
                        self.fetchChanges(withRetryCount: count + 1)
                        return
                    default:
                        return
                    }
                }
                
                self.serverChangeToken = newToken
                if isAll || changedRecords?.count ?? 0 > 0 || deletedRecords?.count ?? 0 > 0 {
                    let changesDict = changedRecords?.reduce([String: Any]()) {
                        (dict, record: CKRecord) in
                        var dict = dict
                        let key = record.recordID.recordName
                        dict[key] = record.object(forKey: key)
                        return dict
                    }
                    let deletedKeys = deletedRecords?.map { $0.recordName }
                    
                    // TODO: Re-enable this after figuring out kv hashable
//                    self.dataStore.mergeRecords(dictionary: changesDict,
//                                                deletedRecordKeys: deletedKeys ?? [],
//                                                containsAllChanges: isAll)
                }
            }
        }
    }
    
    private func handleError(_ error: Error) {
        // Non CKError type -> just log the error
        guard let error = error as? CKError else {
            log.error("Method failed for zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)) with error \(error.localizedDescription, privacy: .public)")
            return
        }
        
        // .zoneNotFound -> delete the zone and set it up again
        if error.containsZoneNotFound() {
            let dispatch = KBTimedDispatch()
            self.serialQueue.async {
                self.deleteRecordZone() { result in
                    if case .failure(let err) = result {
                        log.error("Failed to delete record zone: \(err.localizedDescription, privacy: .public)")
                    }
                    dispatch.semaphore.signal()
                }
            }
            do { try dispatch.wait() }
            catch { log.error("Failed to delete CKRecordZone with error \(error.localizedDescription, privacy: .public)") }
            
            // If the zone was not found and we assumed we had one, reset it
            if self.recordZone != nil {
                self.cleanup()
                self.setupRecordZone()
            } else {
                self.hasSetUpRecordZoneSubscription = false
            }
        }
        
        switch error {
        // .changeTokenExpired -> set the token to nil
        case CKError.changeTokenExpired:
            log.warning("Change token expired for zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)). Setting it to nil")
            self.serverChangeToken = nil
        default:
            log.error("Method failed for zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)) with error \(error.localizedDescription, privacy: .public)")
        }
    }
    
    private func fetchChanges(from token: CKServerChangeToken?,
                              completionHandler: @escaping KBCloudKitManagerFetchRecordsPrivateCompletionBlock) {
        let database = self.container.privateCloudDatabase
        log.trace("Fetching changes in record zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)) in database (\(String(describing: database.databaseScope), privacy: .public))")
        
        var changedRecords: [CKRecord] = []
        var recordIDsToDelete: [CKRecord.ID] = []
        var updatedServerChangeToken: CKServerChangeToken? = nil
        let isCompleteChangeRequest = (token == nil)
        
        let config = CKFetchRecordZoneChangesOperation.ZoneConfiguration(previousServerChangeToken: serverChangeToken,
                                                                         resultsLimit: nil,
                                                                         desiredKeys: nil)
        let operation = CKFetchRecordZoneChangesOperation(recordZoneIDs: [KBCloudKitManager.recordZoneID],
                                                          configurationsByRecordZoneID: [KBCloudKitManager.recordZoneID : config])
        operation.qualityOfService = .utility
        operation.fetchAllChanges = true
        operation.recordChangedBlock = { record in changedRecords.append(record) }
        operation.recordWithIDWasDeletedBlock = { recordId, _ in recordIDsToDelete.append(recordId) }
        operation.recordZoneChangeTokensUpdatedBlock = { _, newToken, _ in updatedServerChangeToken = newToken }
        operation.recordZoneFetchCompletionBlock = { _, newToken, _, _, error in
            if let err = error {
                switch err {
                case CKError.changeTokenExpired:
                    log.warning("Change token expired for zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public))")
                default:
                    log.error("Failed to fetch changes in zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public)) with error \(err.localizedDescription, privacy: .public)")
                }
                updatedServerChangeToken = nil
            } else {
                log.info("Fetched changes successfully in zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public))")
                updatedServerChangeToken = newToken
            }
        }

        operation.fetchRecordZoneChangesCompletionBlock = { error in
            if let error = error {
                log.info("Failed to fetch changes with error \(error.localizedDescription, privacy: .public)")
                self.handleError(error)
            } else {
                log.info("Finished fetching changes in database (\(String(describing: database.databaseScope), privacy: .public)), \(changedRecords.count, privacy: .public)) records")
            }
            
            // If an error was encountered don't claim this was a complete change request (even though it may actually be)
            completionHandler(error,
                              changedRecords,
                              recordIDsToDelete,
                              error == nil ? isCompleteChangeRequest : false,
                              updatedServerChangeToken)
        }
        
        database.add(operation)
    }
    
    
    // MARK: - Public API
    
    public func disableSyncAndDeleteCloudData(completionHandler: @escaping KBActionCompletion) {
        self.serialQueue.async {
            self.cleanup()
            self.deleteRecordZone(completionHandler: completionHandler)
        }
    }
    
    public func fetchChanges(all: Bool = false, completionHandler: KBCloudKitManagerFetchRecordsCompletionBlock) {
        self.serialQueue.async {
            self.fetchChanges(from: all ? nil : self.serverChangeToken) {
                error, savedRecords, deletedRecordsIds, isAll, newToken in
                self.serialQueue.async {
                    if let error = error {
                        self.handleError(error)
                    } else {
                        log.info("Fetched changes successfully in zone (\(KBCloudKitManager.recordZoneID.zoneName, privacy: .public))")
                        self.serverChangeToken = newToken
                    }
                }
            }
        }
    }
    
    private func save(records: [CKRecord]?,
                      recordIDsToDelete: [CKRecord.ID]?,
                      savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
                      completionHandler: @escaping KBCloudKitManagerModifyRecordsCompletionBlock) {
        Dispatch.dispatchPrecondition(condition: .onQueue(self.serialQueue))
        
        guard records?.count ?? 0 > 0, recordIDsToDelete?.count ?? 0 > 0 else {
            completionHandler(nil, nil, nil)
            return
        }
        
        let saveRecordsOperation = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: recordIDsToDelete)
        saveRecordsOperation.isAtomic = true
        saveRecordsOperation.savePolicy = savePolicy
        saveRecordsOperation.modifyRecordsCompletionBlock = {
            savedRecords, deletedRecordsIDs, error in
            if let error = error {
                log.error("Failed to save records into database with error: \(error.localizedDescription, privacy: .public)")
                self.handleError(error)
            } else {
                log.info("Saved \(savedRecords?.count ?? 0, privacy: .public) records into database");
            }
            completionHandler(error, savedRecords, deletedRecordsIDs)
        }
        
        self.container.privateCloudDatabase.add(saveRecordsOperation)
    }
    
    public func saveRecords(withDictionary keysAndValues: [String: Any],
                            completionHandler: @escaping KBCloudKitManagerModifyRecordsCompletionBlock) {
        self.serialQueue.async {
            let records = keysAndValues.map({ (key: String, value: Any?) -> CKRecord in
                let recordID = CKRecord.ID(recordName: key, zoneID: KBCloudKitManager.recordZoneID)
                let record = CKRecord(recordType: kKBCloudKitKeyValueRecordType, recordID: recordID)
                if let value = value {
                    // TODO: Check this conversion here
                    switch(value) {
                    case let stringValue as NSString:
                        record.setObject(stringValue, forKey: key)
                    case let numberValue as NSNumber:
                        record.setObject(numberValue, forKey: key)
                    case let dateValue as NSDate:
                        record.setObject(dateValue, forKey: key)
                    case let arrayValue as NSArray:
                        record.setObject(arrayValue, forKey: key)
                    default:
                        let data: Data
                        data = NSKeyedArchiver.archivedData(withRootObject: value)
                        record.setObject(data as NSData, forKey: key)
                    }
                } else {
                    record.setNilValueForKey(key)
                }
                
                record.setObject(NSNumber.init(value: kKBCloudKitKeyValueRecordTypeVersion),
                                forKey: kKBCloudKitKeyValueRecordTypeVersionKey)
                return record
            })
            
            self.save(records: records, recordIDsToDelete: nil, savePolicy: .changedKeys) { error, savedRecords, deletedRecordsIDs in
                self.serialQueue.async {
                    completionHandler(error, savedRecords, deletedRecordsIDs)
                }
            }
        }
    }
    
    public func removeRecords(forKeys keys: [String], completionHandler: @escaping (Swift.Result<([CKRecord]?, [CKRecord.ID]?),Error>) -> ()) {
        self.serialQueue.async {
            let recordIDsKeys = keys.map { CKRecord.ID(recordName: $0, zoneID: KBCloudKitManager.recordZoneID) }
            self.save(records: nil, recordIDsToDelete: recordIDsKeys, savePolicy: .changedKeys) {
                error, savedRecords, deletedRecordsIDs in
                if let error = error {
                    completionHandler(.failure(error))
                } else {
                    completionHandler(.success((savedRecords, deletedRecordsIDs)))
                }
            }
        }
    }
}
