import Foundation

public protocol SHCollectionSyncingDelegate {

    /// Called when a collection has been created, updated, or deleted on the server
    /// - Parameter collectionId: the ID of the collection that changed
    func didChangeCollection(withId collectionId: String)
}
