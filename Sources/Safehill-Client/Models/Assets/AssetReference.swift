struct AssetReference: GenericAssetIdentifiable {
    
    public let localIdentifier: LocalIdentifier?
    public let globalIdentifier: GlobalIdentifier?
    
    init(localIdentifier: LocalIdentifier) {
        self.localIdentifier = localIdentifier
        self.globalIdentifier = nil
    }
    
    init(globalIdentifier: GlobalIdentifier) {
        self.localIdentifier = nil
        self.globalIdentifier = globalIdentifier
    }
    
}
