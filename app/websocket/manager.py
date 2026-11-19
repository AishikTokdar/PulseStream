import json
from typing import Dict, List, Set
from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        # Maps tenant_id to set of active WebSockets
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # Global connections list for broadcast
        self.global_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket, tenant_id: str = "global") -> None:
        await websocket.accept()
        if tenant_id == "global":
            self.global_connections.add(websocket)
        else:
            if tenant_id not in self.active_connections:
                self.active_connections[tenant_id] = set()
            self.active_connections[tenant_id].add(websocket)

    def disconnect(self, websocket: WebSocket, tenant_id: str = "global") -> None:
        if tenant_id == "global":
            self.global_connections.discard(websocket)
        else:
            if tenant_id in self.active_connections:
                self.active_connections[tenant_id].discard(websocket)
                if not self.active_connections[tenant_id]:
                    del self.active_connections[tenant_id]

    async def send_personal_message(self, message: dict, websocket: WebSocket) -> None:
        await websocket.send_text(json.dumps(message))

    async def broadcast_to_tenant(self, tenant_id: str, message: dict) -> None:
        tenant_conns = self.active_connections.get(tenant_id)
        if not tenant_conns and not self.global_connections:
            return  # Fast path: Zero active listeners, bypass JSON serialization entirely

        payload_str = json.dumps(message)
        # Send to tenant subscribers
        if tenant_conns:
            for connection in list(tenant_conns):
                try:
                    await connection.send_text(payload_str)
                except Exception:
                    self.disconnect(connection, tenant_id)

        # Send to global subscribers
        if self.global_connections:
            for connection in list(self.global_connections):
                try:
                    await connection.send_text(payload_str)
                except Exception:
                    self.disconnect(connection, "global")


ws_manager = ConnectionManager()
