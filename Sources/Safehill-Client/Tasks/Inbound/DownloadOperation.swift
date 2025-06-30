import Foundation

protocol SHDownloadOperation {
    
    func fetchDescriptors(
        filteringAssets globalIdentifiers: [GlobalIdentifier]?,
        filteringGroups groupIds: [String]?,
        after date: Date?,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    )
    
    func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier],
        completionHandler: @escaping (Result<[UserIdentifier: any SHServerUser], Error>) -> Void
    )
    
    func processDescriptors(
        _ descriptors: [any SHAssetDescriptor],
        fromRemote: Bool,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHAssetDescriptor], Error>) -> Void
    )
    
    func createAssetActivities(
        from descriptors: [any SHAssetDescriptor],
        usersDict: [UserIdentifier: any SHServerUser]
    ) async -> [any AssetActivity]
}
