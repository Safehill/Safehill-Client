//
//  DummyDescriptor.swift
//  Safehill-Client
//
//  Created by Gennaro on 5/19/25.
//

@testable import Safehill_Client
import Foundation

struct DummyDescriptor : SHAssetDescriptor {
    var globalIdentifier: GlobalIdentifier
    
    var localIdentifier: LocalIdentifier?
    
    var creationDate: Date?
    
    var uploadState: SHAssetUploadState
    
    var sharingInfo: any SHDescriptorSharingInfo
    
    func serialized() -> SHGenericAssetDescriptorClass {
        SHGenericAssetDescriptorClass(
            globalIdentifier: self.globalIdentifier,
            localIdentifier: self.localIdentifier,
            creationDate: self.creationDate,
            uploadState: self.uploadState,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: "",
                groupIdsByRecipientUserIdentifier: [:],
                groupInfoById: [:]
            )
        )
    }

    init(globalIdentifier: GlobalIdentifier) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = nil
        self.creationDate = nil
        self.uploadState = .completed
        self.sharingInfo = SHGenericDescriptorSharingInfo(
            sharedByUserIdentifier: "",
            groupIdsByRecipientUserIdentifier: [:],
            groupInfoById: [:]
        )
    }
}
