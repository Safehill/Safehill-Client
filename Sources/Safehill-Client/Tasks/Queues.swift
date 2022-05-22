import KnowledgeBase

public let EncryptionQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetEncryptionQueue", type: .fifo)

public let UploadQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetUploadQueue", type: .fifo)

public let ShareQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetShareQueue", type: .fifo)

public let UploadHistoryQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetUploadHistoryQueue", type: .fifo)

public let FailedUploadQueue = KBQueueStore.store(withName: "com.gf.enkey.PhotoAssetFailedUploadQueue", type: .fifo)
