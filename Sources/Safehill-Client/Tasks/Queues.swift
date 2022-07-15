import KnowledgeBase

public let FetchQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetFetchQueue", type: .fifo)

public let EncryptionQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetEncryptionQueue", type: .fifo)

public let UploadQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetUploadQueue", type: .fifo)

public let ShareQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetShareQueue", type: .fifo)

public let UploadHistoryQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetUploadHistoryQueue", type: .fifo)

public let ShareHistoryQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetShareHistoryQueue", type: .fifo)

public let FailedUploadQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetFailedUploadQueue", type: .fifo)

public let FailedShareQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetFailedShareQueue", type: .fifo)

public let DownloadQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetDownloadQueue", type: .fifo)
