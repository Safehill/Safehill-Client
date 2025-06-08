public protocol SHAssetsDescriptorsDelegate : SHInboundAssetOperationDelegate {
    
    func didUpdateAssets(with globalIdentifiers: [GlobalIdentifier])
}
