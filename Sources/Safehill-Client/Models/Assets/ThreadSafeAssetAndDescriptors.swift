public struct AssetAndDescriptor {
    let asset: any SHDecryptedAsset
    let descriptor: any SHAssetDescriptor
}

actor ThreadSafeAssetAndDescriptors {
    var list: [AssetAndDescriptor]
    
    init(list: [AssetAndDescriptor]) {
        self.list = list
    }
    
    func add(_ value: AssetAndDescriptor) {
        self.list.append(value)
    }
}

