import Photos
import Foundation
#if os(iOS)
import UIKit
#elseif os(macOS)
import AppKit
#endif
import Safehill_Crypto


private let PHAssetIdentifierKey = "phAssetLocalIdentifier"
private let CalculatedGlobalIdentifier = "calculatedGlobalIdentifier"


public class SHApplePhotoAsset : NSObject, NSSecureCoding {
    public static var supportsSecureCoding = true
    
    private var calculatedGlobalIdentifier: GlobalIdentifier? = nil
    
    var imageManager: PHImageManager
    
    public let phAsset: PHAsset
    
    internal func setGlobalIdentifier(_ gid: GlobalIdentifier) throws {
        if self.calculatedGlobalIdentifier == nil {
            self.calculatedGlobalIdentifier = gid
        } else {
            if self.calculatedGlobalIdentifier != gid {
                throw SHBackgroundOperationError.fatalError("previously generated global identifier doesn't match the one provided")
            }
        }
    }
    
    public init(for asset: PHAsset,
                usingCachingImageManager manager: PHCachingImageManager? = nil) {
        self.phAsset = asset
        self.imageManager = manager ?? PHImageManager.default()
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let phAssetIdentifier = decoder.decodeObject(of: NSString.self, forKey: PHAssetIdentifierKey)
        let calculatedGlobalId = decoder.decodeObject(of: NSString.self, forKey: CalculatedGlobalIdentifier)
        
        guard let phAssetIdentifier = phAssetIdentifier as? String else {
            log.error("unexpected value for phAssetIdentifier when decoding SHApplePhotoAsset object")
            return nil
        }
        
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [phAssetIdentifier], options: nil)
        guard fetchResult.count > 0 else {
            return nil
        }
        
        let asset = fetchResult.object(at: 0)
        self.init(for: asset)
        if let calculatedGlobalId = calculatedGlobalId as? GlobalIdentifier {
            self.calculatedGlobalIdentifier = calculatedGlobalId
        }
    }
    
    ///
    ///  **Use it carefully!!**
    ///  This operation can take time, as it needs to retrieve the full resolution asset from the Apple Photos library.
    ///
    /// - Returns:
    ///   - a hash representing the image fingerprint, that can be used as a unique global identifier
    func retrieveOrGenerateGlobalIdentifier() throws -> GlobalIdentifier {
        guard calculatedGlobalIdentifier == nil else {
            return calculatedGlobalIdentifier!
        }
        
        let start = CFAbsoluteTimeGetCurrent()
        
        let targetSize: CGSize
        if self.phAsset.pixelWidth > self.phAsset.pixelHeight {
            targetSize = CGSize(width: imageSizeForGlobalIdCalculation.width,
                                height: floor(imageSizeForGlobalIdCalculation.width * Double(self.phAsset.pixelHeight)/Double(self.phAsset.pixelWidth)))
        } else {
            targetSize = CGSize(width: floor(imageSizeForGlobalIdCalculation.height * Double(self.phAsset.pixelWidth)/Double(self.phAsset.pixelHeight)),
                                height: imageSizeForGlobalIdCalculation.height)
        }
        
        var error: Error? = nil

        self.phAsset.data(
            forSize: targetSize,
            usingImageManager: self.imageManager,
            synchronousFetch: true,
            deliveryMode: .highQualityFormat,
            resizeMode: .exact
        ) { result in
            switch result {
            case .failure(let err):
                error = err
            case .success(let data):
                let hash = SHHash.stringDigest(for: data)
                
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to generate an asset global identifier")
                
                self.calculatedGlobalIdentifier = hash
            }
        }
        
        guard error == nil else {
            throw error!
        }
        return self.calculatedGlobalIdentifier!
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.phAsset.localIdentifier, forKey: PHAssetIdentifierKey)
        coder.encode(self.calculatedGlobalIdentifier, forKey: CalculatedGlobalIdentifier)
    }
    
}
