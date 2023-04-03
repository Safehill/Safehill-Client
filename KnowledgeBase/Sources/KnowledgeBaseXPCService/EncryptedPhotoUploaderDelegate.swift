//
//  EncryptedPhotoUploaderDelegate.swift
//  
//
//  Created by Gennaro Frazzingaro on 8/11/21.
//

import Foundation
import KnowledgeBase
import Photos

struct EncryptedPhotoUploaderDelegate {
    
    func encrypt(data: Data) -> Data {
        return data
    }
    
    func upload(data: Data, completionHandler: (Swift.Result<Void, Error>) -> ()) {
        // Call the lambda with the data encrypted locally
    }
    
    func getPictureAsData(from asset: PHAsset, completionHandler: (Swift.Result<Data, Error>) -> ()) {
        let options = PHImageRequestOptions()
        options.isSynchronous = false
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.version = .current
        PHImageManager.default().requestImageDataAndOrientation(for: asset, options: options) {
            data, dataUTI, orientation, resultInfoKeys in
            
        }
    }
}
