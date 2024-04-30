import Foundation
import os
import KnowledgeBase

public protocol SHBackgroundOperationProtocol : Operation {
    
    associatedtype OperationResult
    
    var log: Logger { get }
    
    func run(qos: DispatchQoS.QoSClass, completionHandler: @escaping (OperationResult) -> Void)
}

public protocol SHBackgroundQueueBackedOperationProtocol : SHBackgroundOperationProtocol {
    
    func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem
    
    func process(
        _: KBQueueItem,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    )
}
