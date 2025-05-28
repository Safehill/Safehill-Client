import Foundation
import CoreML
import ZIPFoundation
#if os(iOS)
import UIKit
#elseif os(macOS)
import AppKit
#endif

enum ModelVariant {
    case mlProgram
    case neuralNetwork

    static func current() -> ModelVariant {
        if #available(iOS 15.0, macOS 12.0, *) {
            return .mlProgram
        }
        else {
            return .neuralNetwork
        }
    }

    var s3URL: URL {
        switch self {
        case .mlProgram:
            return URL(string: "https://s3.us-east-2.wasabisys.com/safehill-ml-prod/v1/TinyCLIP.mlpackage.zip")!
        case .neuralNetwork:
            return URL(string: "https://s3.us-east-2.wasabisys.com/safehill-ml-prod/v1/compat/TinyCLIP.mlmodel")!
        }
    }

    var modelFileName: String {
        switch self {
        case .mlProgram: return "TinyCLIP.mlpackage"
        case .neuralNetwork: return "TinyCLIP.mlmodel"
        }
    }
    
    var localURL: URL {
        let fileManager = FileManager.default
        let cacheDir = fileManager.urls(for: .cachesDirectory, in: .userDomainMask)[0]
        switch self {
        case .mlProgram:
            return cacheDir
                .appendingPathComponent(self.modelFileName)
        case .neuralNetwork:
            return cacheDir
                .appendingPathComponent("compat")
                .appendingPathComponent(self.modelFileName)
        }
    }
}

public actor SHAssetEmbeddingsController {
    
    public static let shared = SHAssetEmbeddingsController()

    private var model: MLModel?

    // MARK: - Public methods

    internal func loadModelIfNeeded() async throws {
        let variant = ModelVariant.current()
        let fileManager = FileManager.default
        let localModelURL = variant.localURL

        if !fileManager.fileExists(atPath: localModelURL.path) {
            try await downloadAndCacheModel(from: variant.s3URL, to: localModelURL)
        }

        // Compile .mlmodel to .mlmodelc
        let compiledURL = try MLModel.compileModel(at: localModelURL)

        model = try MLModel(contentsOf: compiledURL)
    }
    
    public func generateEmbeddings(for nsuiImage: NSUIImage) async throws -> String {
        let embeddings = try runModel(on: nsuiImage)
        let base64Embeddings = try embeddingToBase64(embeddings)
        return base64Embeddings
    }

    // MARK: - Private methods

    private func downloadAndCacheModel(from url: URL, to destination: URL) async throws {
        let fileManager = FileManager.default
        let tempURL: URL
        let response: URLResponse
        
        if #available(iOS 15.0, macOS 12.0, *) {
            (tempURL, response) = try await URLSession.shared.download(from: url)
        } else {
            (tempURL, response) = try await downloadFile(from: url)
        }
        
        guard let httpResponse = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }

        guard (200...299).contains(httpResponse.statusCode) else {
            throw NSError(
                domain: "TinyCLIP",
                code: httpResponse.statusCode,
                userInfo: [NSLocalizedDescriptionKey: "Failed to download model at \(url). HTTP \(httpResponse.statusCode)"])
        }
        
        if url.pathExtension == "zip" {
            if fileManager.fileExists(atPath: destination.path) {
                try fileManager.removeItem(at: destination)
            }
            try fileManager.unzipItem(at: tempURL, to: destination.deletingLastPathComponent())
        } else {
            try FileManager.default.copyItem(at: tempURL, to: destination)
        }
    }

    private func downloadFile(from url: URL) async throws -> (URL, URLResponse) {
        try await withCheckedThrowingContinuation { continuation in
            let task = URLSession.shared.downloadTask(with: url) { tempURL, response, error in
                if let error = error {
                    continuation.resume(throwing: error)
                    return
                }

                guard let tempURL = tempURL,
                      let response = response
                else {
                    continuation.resume(throwing: NSError(
                        domain: "TinyCLIP",
                        code: 5,
                        userInfo: [NSLocalizedDescriptionKey: "No file was downloaded"]
                    ))
                    return
                }

                continuation.resume(returning: (tempURL, response))
            }
            task.resume()
        }
    }

    private func runModel(on image: NSUIImage) throws -> MLMultiArray {
        guard let model = model else {
            throw NSError(domain: "TinyCLIP", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "Model not loaded"
            ])
        }

        guard let buffer = image.toCVPixelBuffer(width: 224, height: 224) else {
            throw NSError(domain: "TinyCLIP", code: 3, userInfo: [
                NSLocalizedDescriptionKey: "Failed to create pixel buffer"
            ])
        }

        let input = try MLDictionaryFeatureProvider(dictionary: ["input": buffer])
        let result = try model.prediction(from: input)

        guard let embedding = result.featureValue(for: "output")?.multiArrayValue else {
            throw NSError(domain: "TinyCLIP", code: 4, userInfo: [
                NSLocalizedDescriptionKey: "Model did not return embedding"
            ])
        }

        return embedding
    }
    
    private func embeddingToBase64(_ embedding: MLMultiArray) throws -> String {
        // Ensure it's float32
        guard embedding.dataType == .float32 else {
            throw NSError(domain: "TinyCLIP", code: 5, userInfo: [
                NSLocalizedDescriptionKey: "Expected float32 MLMultiArray"
            ])
        }

        // Flatten MLMultiArray to raw Float32 bytes
        let count = embedding.count
        let floatPtr = UnsafeMutablePointer<Float32>.allocate(capacity: count)
        defer { floatPtr.deallocate() }

        // Copy MLMultiArray data into buffer
        memcpy(floatPtr, embedding.dataPointer, count * MemoryLayout<Float32>.stride)

        // Wrap in Data and encode to Base64
        let data = Data(bytes: floatPtr, count: count * MemoryLayout<Float32>.stride)
        return data.base64EncodedString()
    }
}
