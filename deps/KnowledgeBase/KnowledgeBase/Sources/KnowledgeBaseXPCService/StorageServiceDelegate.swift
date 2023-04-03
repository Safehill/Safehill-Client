//
//  StorageServiceDelegate.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/29/21.
//

import Foundation
import KnowledgeBase

class KBStorageServiceDelegate : NSObject, NSXPCListenerDelegate {
    func listener(_ listener: NSXPCListener, shouldAcceptNewConnection newConnection: NSXPCConnection) -> Bool {
        newConnection.exportedInterface = NSXPCInterface(with: KBStorageXPCProtocol.self)

        let exportedObject = KBStorageServiceProviderXPC()
        newConnection.exportedObject = exportedObject
        newConnection.resume()
        return true
    }
}
