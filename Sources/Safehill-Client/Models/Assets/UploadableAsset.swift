import Photos
import Foundation
import Safehill_Crypto


public class SHUploadableAsset : NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding = true
    
    public let localIdentifier: LocalIdentifier?
    public let globalIdentifier: GlobalIdentifier
    public var fingerprint: AssetFingerprint?
    public let creationDate: Date?
    public let data: [SHAssetQuality: Data]
    
    enum CodingKeys: String, CodingKey {
        case localIdentifier
        case globalIdentifier
        case perceptualHash
        case embeddings
        case creationDate
        case data
    }
    
    public init(
        localIdentifier: LocalIdentifier?,
        globalIdentifier: GlobalIdentifier,
        fingerprint: AssetFingerprint? = nil,
        creationDate: Date?,
        data: [SHAssetQuality: Data]
    ) {
        self.localIdentifier = localIdentifier
        self.globalIdentifier = globalIdentifier
        self.fingerprint = fingerprint
        self.creationDate = creationDate
        self.data = data
    }
    
    internal func calculateFingerprintIfNeeded() async throws {
        if self.fingerprint == nil {
            guard let lowResData = self.data[.lowResolution] else {
                throw SHBackgroundOperationError.fatalError("Asked to calculate a fingeprint for an asset that does not have low resolution data. This isn't supposed to happen")
            }
            
            guard let image = NSUIImage.from(data: lowResData) else {
                throw SHHashingController.Error.noImageData
            }
            
            let embeddingsController = SHAssetEmbeddingsController.shared
            
            let pHash = try SHHashingController.perceptualHash(for: image)
            let base64Embeddings: String = try await embeddingsController.generateEmbeddings(for: image)
            self.fingerprint = AssetFingerprint(
                perceptualHash: pHash,
                embeddings: base64Embeddings
            )
        }
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let localIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue)
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let perceptualHash = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.perceptualHash.rawValue)
        let embeddings = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.embeddings.rawValue)
        let creationDateStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.creationDate.rawValue) as? String
        
        var dataDict = [SHAssetQuality: Data]()
        for quality in SHAssetQuality.all {
            if let qdata = decoder.decodeObject(of: NSData.self, forKey: CodingKeys.data.rawValue + "::" + quality.rawValue) as? Data {
                dataDict[quality] = qdata
            }
        }
        
        guard let globalIdentifier = globalIdentifier as? GlobalIdentifier else {
            log.error("unexpected value for globalIdentifier when decoding SHUploadableAsset object")
            return nil
        }
        
        let fingerprint: AssetFingerprint?
        if let embeddings = embeddings as? String {
            fingerprint = AssetFingerprint(
                perceptualHash: perceptualHash as? String,
                embeddings: embeddings
            )
        } else {
            fingerprint = nil
        }
        
        let creationDate: Date?
        if let creationDateStr {
            guard let date = creationDateStr.iso8601withFractionalSeconds else {
                log.error("unexpected value for creationDate when decoding SHUploadableAsset object")
                return nil
            }
            creationDate = date
        } else {
            creationDate = nil
        }
        
        self.init(
            localIdentifier: localIdentifier as? LocalIdentifier,
            globalIdentifier: globalIdentifier,
            fingerprint: fingerprint,
            creationDate: creationDate,
            data: dataDict
        )
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        if let pHash = self.fingerprint?.perceptualHash {
            coder.encode(pHash, forKey: CodingKeys.perceptualHash.rawValue)
        }
        if let embeddings = self.fingerprint?.embeddings {
            coder.encode(embeddings, forKey: CodingKeys.embeddings.rawValue)
        }
        coder.encode(self.creationDate?.iso8601withFractionalSeconds, forKey: CodingKeys.creationDate.rawValue)
        
        for (version, data) in self.data {
            coder.encode(data, forKey: CodingKeys.data.rawValue + "::" + version.rawValue)
        }
    }
}


