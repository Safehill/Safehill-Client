import Foundation
import CoreML
import CryptoKit
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
#if targetEnvironment(simulator)
        // Always fallback to neuralNetwork on simulator
        return .neuralNetwork
#else
        if #available(iOS 15.0, macOS 12.0, *) {
            return .mlProgram
        } else {
            return .neuralNetwork
        }
#endif
    }

    var s3URL: URL {
        switch self {
        case .mlProgram:
            return URL(string: "https://s3.us-east-2.wasabisys.com/safehill-ml-prod/latest/TinyCLIP.mlpackage.zip")!
        case .neuralNetwork:
            return URL(string: "https://s3.us-east-2.wasabisys.com/safehill-ml-prod/latest/compat/TinyCLIP.mlmodel")!
        }
    }
    
    var checksumS3URL: URL {
        switch self {
        case .mlProgram:
            return URL(string: "https://s3.us-east-2.wasabisys.com/safehill-ml-prod/latest/TinyCLIP.mlpackage.zip.sha256")!
        case .neuralNetwork:
            return URL(string: "https://s3.us-east-2.wasabisys.com/safehill-ml-prod/latest/compat/TinyCLIP.mlmodel.sha256")!
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
    
    private init() {
        self.model = nil
    }

    // MARK: - Public methods

    public func loadModelIfNeeded() async throws {
        let variant = ModelVariant.current()
        let fileManager = FileManager.default
        let localModelURL = variant.localURL
        
        let needsDownload: Bool

        if fileManager.fileExists(atPath: localModelURL.path) {
            do {
                let expectedChecksum = try await downloadChecksum(from: variant.checksumS3URL)
                let localChecksum = try sha256(for: localModelURL)
                needsDownload = (expectedChecksum != localChecksum)
            } catch {
                // If we can't fetch the checksum or calculate it, assume it's invalid
                log.error("Checksum verification failed, will re-download ML model: \(error)")
                needsDownload = true
            }
        } else {
            needsDownload = true
        }

        if needsDownload {
            try? fileManager.removeItem(atPath: localModelURL.path)
            try await downloadAndCacheModel(from: variant.s3URL, to: localModelURL)
        }

        // Compile .mlmodel to .mlmodelc
        let compiledURL = try MLModel.compileModel(at: localModelURL)

        self.model = try MLModel(contentsOf: compiledURL)
    }
    
    public func generateEmbeddings(for nsuiImage: NSUIImage) async throws -> String {
        let embeddings = try runModel(on: nsuiImage)
        let base64Embeddings = try embeddingToBase64(embeddings)
        return base64Embeddings
    }

    // MARK: - Private methods
    
    private func sha256(for fileURL: URL) throws -> String {
        let data = try Data(contentsOf: fileURL)
        let hash = SHA256.hash(data: data)
        return hash.compactMap { String(format: "%02x", $0) }.joined()
    }
    
    private func downloadChecksum(from url: URL) async throws -> String {
        let (data, _) = try await URLSession.shared.data(from: url)
        guard let checksum = String(data: data, encoding: .utf8)?
                .trimmingCharacters(in: .whitespacesAndNewlines),
              checksum.count == 64
        else {
            throw NSError(domain: "TinyCLIP", code: 99, userInfo: [
                NSLocalizedDescriptionKey: "Invalid checksum format from \(url)"
            ])
        }
        return checksum
    }

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
        
        try fileManager.createDirectory(at: destination.deletingLastPathComponent(), withIntermediateDirectories: true)
        
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
        guard let model = self.model else {
            throw NSError(domain: "TinyCLIP", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "Model not loaded"
            ])
        }

        let inputArray = try image.toNormalizedNCHWArray()
        let input = try MLDictionaryFeatureProvider(dictionary: ["pixel_values": inputArray])
        let result = try model.prediction(from: input)
        
        guard result.featureNames.count == 1,
              let featureName = result.featureNames.first,
              let embedding = result.featureValue(for: featureName)?.multiArrayValue else {
            throw NSError(domain: "TinyCLIP", code: 4, userInfo: [
                NSLocalizedDescriptionKey: "Model did not return expected embedding"
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
