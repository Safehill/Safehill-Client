import Foundation

public struct SHGenericAssetDescriptor : SHAssetDescriptor, Codable {
    public let globalIdentifier: GlobalIdentifier
    public var localIdentifier: LocalIdentifier?
    public let creationDate: Date?
    public let uploadState: SHAssetDescriptorUploadState
    public let sharingInfo: SHDescriptorSharingInfo
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case localIdentifier
        case creationDate
        case uploadState
        case sharingInfo
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(globalIdentifier, forKey: .globalIdentifier)
        try container.encode(localIdentifier, forKey: .localIdentifier)
        try container.encode(creationDate, forKey: .creationDate)
        try container.encode(uploadState.rawValue, forKey: .uploadState)
        try container.encode(sharingInfo as! SHGenericDescriptorSharingInfo, forKey: .sharingInfo)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        globalIdentifier = try container.decode(String.self, forKey: .globalIdentifier)
        localIdentifier = try? container.decode(String.self, forKey: .localIdentifier)
        let dateString = try? container.decode(String.self, forKey: .creationDate)
        creationDate = dateString?.iso8601withFractionalSeconds
        let uploadStateString = try container.decode(String.self, forKey: .uploadState)
        guard let uploadState = SHAssetDescriptorUploadState(rawValue: uploadStateString) else {
            throw DecodingError.dataCorrupted(.init(codingPath: [CodingKeys.uploadState],
                                                    debugDescription: "Invalid UploadState value \(uploadStateString)")
            )
        }
        self.uploadState = uploadState
        sharingInfo = try container.decode(SHGenericDescriptorSharingInfo.self, forKey: .sharingInfo)
    }
    
    public init(globalIdentifier: GlobalIdentifier,
                localIdentifier: LocalIdentifier?,
                creationDate: Date?,
                uploadState: SHAssetDescriptorUploadState,
                sharingInfo: SHDescriptorSharingInfo) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.uploadState = uploadState
        self.sharingInfo = sharingInfo
    }
    
    public func serialized() -> SHGenericAssetDescriptorClass {
        SHGenericAssetDescriptorClass(
            globalIdentifier: self.globalIdentifier,
            localIdentifier: self.localIdentifier,
            creationDate: self.creationDate,
            uploadState: self.uploadState,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: self.sharingInfo.sharedByUserIdentifier,
                groupIdsByRecipientUserIdentifier: self.sharingInfo.groupIdsByRecipientUserIdentifier,
                groupInfoById: self.sharingInfo.groupInfoById
            )
        )
    }
    
    internal static func from(_ cls: SHGenericAssetDescriptorClass) -> SHGenericAssetDescriptor {
        SHGenericAssetDescriptor(
            globalIdentifier: cls.globalIdentifier,
            localIdentifier: cls.localIdentifier,
            creationDate: cls.creationDate,
            uploadState: cls.uploadState,
            sharingInfo: cls.sharingInfo
        )
    }
}

