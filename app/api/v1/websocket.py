from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.websocket.manager import ws_manager

router = APIRouter(prefix="/ws", tags=["Real-time WebSockets"])


@router.websocket("/events")
async def websocket_event_stream(
    websocket: WebSocket, tenant_id: str = Query("global")
) -> None:
    await ws_manager.connect(websocket, tenant_id=tenant_id)
    try:
        await websocket.send_json(
            {
                "type": "connection_established",
                "tenant_id": tenant_id,
                "message": f"Subscribed to event feed for tenant '{tenant_id}'",
            }
        )
        while True:
            # Keep connection open and accept incoming ping/pong or control messages
            data = await websocket.receive_text()
            # Echo or respond to ping
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, tenant_id=tenant_id)
    except Exception:
        ws_manager.disconnect(websocket, tenant_id=tenant_id)
