extension SHGlobalSyncOperation: WebSocketDelegate {
    
    ///
    /// CATCH-UP!
    /// Every time the socket connects or re-connects,
    /// make sure the local server is in sync with the remote server.
    /// Only while the WS connection is connected changes on the server
    /// are synced via handling of the Websocket message.
    ///
    public func didConnect() {
        Task {
            do {
                try await self.syncAllAssets()
                try await self.syncInteractionsSummaries()
                log.debug("[SHInteractionsSyncOperation] done syncing interaction summaries")
            } catch {
                log.error("\(error.localizedDescription)")
            }
        }
    }
    
    public func didDisconnect(error: Error?) {}
    
}
