import Foundation

public protocol SHAssetActivityRestorationDelegate {
    
    /// Provide the descriptors for items uploaded by this user, but not shared
    /// - Parameter from: the descriptors
    func restoreUploadQueueItems(from: [String: [(SHUploadHistoryItem, Date)]])
    
    /// Provide the descriptors shared by this user with other users
    /// - Parameter from: the descriptors
    func restoreShareQueueItems(from: [String: [(SHShareHistoryItem, Date)]])
}
