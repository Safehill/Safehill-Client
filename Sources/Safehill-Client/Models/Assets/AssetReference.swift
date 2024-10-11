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

extension AssetReference: CustomStringConvertible {
    public var description: String {
        return "l=\(self.localIdentifier ?? "nil") g=\(self.globalIdentifier ?? "nil")"
    }
}

extension Asset {
    public func idReference() -> AssetReference {
        AssetReference(
            localIdentifier: self.localIdentifier,
            globalIdentifier: self.globalIdentifier
        )
    }
}
