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
    
    public init(localIdentifier: LocalIdentifier?, globalIdentifier: GlobalIdentifier?) {
        assert(localIdentifier != nil || globalIdentifier != nil)
        self.localIdentifier = localIdentifier
        self.globalIdentifier = globalIdentifier
    }
}

extension Asset {
    func idReference() -> AssetReference {
        AssetReference(
            localIdentifier: self.localIdentifier,
            globalIdentifier: self.globalIdentifier
        )
    }
}
