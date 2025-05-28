//
//  DummyAsset.swift
//  Safehill-Client
//
//  Created by Gennaro on 5/19/25.
//

@testable import Safehill_Client
import Foundation

struct DummyAsset : SHEncryptedAsset {
    
    var globalIdentifier: GlobalIdentifier
    
    var localIdentifier: LocalIdentifier? { nil }
    
    var fingerprint: String { "" }
    
    var creationDate: Date? { Date() }
    
    var encryptedVersions: [SHAssetQuality : any SHEncryptedAssetVersion]
    
    init(globalIdentifier: GlobalIdentifier, versions: [SHAssetQuality : any SHEncryptedAssetVersion])
    {
        self.globalIdentifier = globalIdentifier
        self.encryptedVersions = versions
    }
}
