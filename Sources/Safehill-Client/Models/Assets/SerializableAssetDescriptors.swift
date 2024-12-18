import Foundation

/// A class (not a swift struct, such as SHRemoteUser) for SHServer objects
/// to conform to NSSecureCoding, and safely store sharing information in the KBStore.
/// This serialization method is  relevant when storing SHGroupableUploadQueueItem
/// in the queue, and hold user sharing information.
public class SHGenericAssetDescriptorClass: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let globalIdentifier: GlobalIdentifier
    public let localIdentifier: LocalIdentifier?
    public let fingerprint: PerceptualHash
    public let creationDate: Date?
    public let uploadState: SHAssetDescriptorUploadState
    public let sharingInfo: SHDescriptorSharingInfo
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case localIdentifier
        case fingerprint
        case creationDate
        case uploadState
        case sharingInfo
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        coder.encode(self.fingerprint, forKey: CodingKeys.fingerprint.rawValue)
        coder.encode(self.creationDate, forKey: CodingKeys.creationDate.rawValue)
        coder.encode(self.uploadState.rawValue, forKey: CodingKeys.uploadState.rawValue)
        let encodableSharingInfo = try? JSONEncoder().encode(self.sharingInfo as! SHGenericDescriptorSharingInfo)
        coder.encode(encodableSharingInfo, forKey: CodingKeys.sharingInfo.rawValue)
    }
    
    public init(globalIdentifier: GlobalIdentifier,
                localIdentifier: LocalIdentifier?,
                fingerprint: PerceptualHash,
                creationDate: Date?,
                uploadState: SHAssetDescriptorUploadState,
                sharingInfo: SHGenericDescriptorSharingInfo) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.fingerprint = fingerprint
        self.creationDate = creationDate
        self.uploadState = uploadState
        self.sharingInfo = sharingInfo
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let localIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue)
        let fingerprint = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.fingerprint.rawValue)
        let creationDate = decoder.decodeObject(of: NSDate.self, forKey: CodingKeys.creationDate.rawValue)
        let uploadStateStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.uploadState.rawValue)
        let sharingInfoData = decoder.decodeObject(of: NSData.self, forKey: CodingKeys.sharingInfo.rawValue)
                
        guard let globalIdentifier = globalIdentifier as? String else {
            log.error("unexpected value for globalIdentifier when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        guard let fingerprint = fingerprint as? String else {
            log.error("unexpected value for perceptualHash when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        guard let uploadStateStr = uploadStateStr as? String,
              let uploadState = SHAssetDescriptorUploadState(rawValue: uploadStateStr)
        else {
            log.error("unexpected value for uploadState when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        guard let sharingInfoData = sharingInfoData as? Data,
              let sharingInfo = try? JSONDecoder().decode(SHGenericDescriptorSharingInfo.self, from: sharingInfoData) else {
            log.error("unexpected value for sharingInfo when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        
        self.init(
            globalIdentifier: globalIdentifier,
            localIdentifier: localIdentifier as? LocalIdentifier,
            fingerprint: fingerprint,
            creationDate: creationDate as? Date,
            uploadState: uploadState,
            sharingInfo: sharingInfo
        )
    }
}
