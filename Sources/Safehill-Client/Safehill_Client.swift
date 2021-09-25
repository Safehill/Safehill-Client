//
//  Safehill_Client.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/23/21.
//

import Photos.PHImageManager

public let kSHLowResPictureSize = CGSize(width: 240.0, height: 240.0)

public enum SHAssetFetchError : Error {
    case unexpectedData(Any?)
    case fatalError(String)
}


// https://feedbackassistant.apple.com/feedback/9649898
// Apparently Swift has trouble linking this particular method from the Photos framework from a package
// Moving this (and the related SHUploadOperation) to the app momentarily

//extension PHAsset {
//    /// Get the asset data from a lazy-loaded PHAsset object
//    /// - Parameters:
//    ///   - asset: the PHAsset object
//    ///   - size: the size of the asset
//    ///   - imageManager: the image manager to use (useful in case of a PHCachedImage manager)
//    ///   - synchronousFetch: determines how many times the completionHandler is called. Asynchronous fetching may call the completion handler multiple times with lower resolution version of the requested asset as soon as it's ready
//    ///   - completionHandler: the completion handler
//    func data(forSize size: CGSize? = nil,
//              usingImageManager imageManager: PHImageManager,
//              synchronousFetch: Bool,
//              completionHandler: @escaping (Swift.Result<Data, Error>) -> ()) {
//        let option = PHImageRequestOptions()
//        option.isSynchronous = synchronousFetch
//
//        let targetSize = size ?? CGSize(width: self.pixelWidth, height: self.pixelHeight)
//
//        switch self.mediaType {
//        case .image:
//            imageManager.requestImage(for: self, targetSize: targetSize, contentMode: PHImageContentMode.aspectFill, options: option) {
//                image, _ in
//    #if os(iOS)
//                if let image = image,
//                   let data = image.pngData() {
//                    completionHandler(.success(data))
//                    return
//                }
//    #else
//                if let image = image,
//                   let data = image.png {
//                    completionHandler(.success(data))
//                    return
//                }
//    #endif
//                completionHandler(.failure(SHAssetFetchError.unexpectedData(image)))
//            }
//        case .video:
//            imageManager.requestAVAsset(forVideo: self, options: nil) { asset, audioMix, info in
//                if let asset = asset as? AVURLAsset,
//                   let data = NSData(contentsOf: asset.url) as Data? {
//                    completionHandler(.success(data))
//                } else {
//                    completionHandler(.failure(SHAssetFetchError.unexpectedData(asset)))
//                }
//            }
//        default:
//            completionHandler(.failure(SHAssetFetchError.fatalError("PHAsset mediaType not supported \(self.mediaType)")))
//        }
//    }
//}
