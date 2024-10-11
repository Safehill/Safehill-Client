public struct AssetReference: GenericAssetIdentifiable {
    
    public let localIdentifier: LocalIdentifier?
    public let globalIdentifier: GlobalIdentifier?
    
    public init(localIdentifier: LocalIdentifier) {
        self.localIdentifier = localIdentifier
        self.globalIdentifier = nil
    }
    
    public init(globalIdentifier: GlobalIdentifier) {
        self.localIdentifier = nil
        self.globalIdentifier = globalIdentifier
    }
    
}
