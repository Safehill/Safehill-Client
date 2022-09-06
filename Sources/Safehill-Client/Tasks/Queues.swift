import KnowledgeBase

public let FetchQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetFetchQueue", type: .fifo)

public let EncryptionQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetEncryptionQueue", type: .fifo)

public let UploadQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetUploadQueue", type: .fifo)

public let ShareQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetShareQueue", type: .fifo)

public let UploadHistoryQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetUploadHistoryQueue", type: .fifo)

public let ShareHistoryQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetShareHistoryQueue", type: .fifo)

public let FailedUploadQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetFailedUploadQueue", type: .fifo)

public let FailedShareQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetFailedShareQueue", type: .fifo)

public let DownloadQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetDownloadQueue", type: .fifo)


// Asset <=> User knowledge graph

public let KnowledgeGraph = KBKnowledgeStore.store(withName: "com.gf.safehill.KnowledgeGraph")

public enum KGPredicates: String {
    case shares = "shares"
    case knows = "knows"
    case sharedWith = "sharedWith"
}
