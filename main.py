import http
import asyncio
import websockets
import json
import os
import dotenv
from PSQLConnector.connector import PSQLConnection as db

dotenv.load_dotenv()
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8765))

print(os.environ.get("DB_HOSTNAME"))
db.connect(
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    host=os.environ.get("DB_HOSTNAME"),
    database=os.environ.get("DB"),
)

SUBSCRIBERS_BY_MATCH = {}
CONNECTION_SUBSCRIPTIONS = {}


async def health_check(path, request_headers):
    if path in ("/", "/healthz"):
        return http.HTTPStatus.OK, [("Content-Type", "text/plain")], b"OK\n"
    return None


def _parse_match_id(raw_value):
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _add_subscription(websocket, match_id):
    if match_id not in SUBSCRIBERS_BY_MATCH:
        SUBSCRIBERS_BY_MATCH[match_id] = set()
    SUBSCRIBERS_BY_MATCH[match_id].add(websocket)

    if websocket not in CONNECTION_SUBSCRIPTIONS:
        CONNECTION_SUBSCRIPTIONS[websocket] = set()
    CONNECTION_SUBSCRIPTIONS[websocket].add(match_id)


async def _remove_connection(websocket):
    subscribed = CONNECTION_SUBSCRIPTIONS.pop(websocket, set())
    for match_id in subscribed:
        subscribers = SUBSCRIBERS_BY_MATCH.get(match_id)
        if not subscribers:
            continue

        subscribers.discard(websocket)
        if not subscribers:
            SUBSCRIBERS_BY_MATCH.pop(match_id, None)


async def _broadcast_to_match_ids(match_ids, payload):
    if not match_ids:
        return

    targets = set()
    for match_id in match_ids:
        targets.update(SUBSCRIBERS_BY_MATCH.get(match_id, set()))

    if not targets:
        return

    encoded = json.dumps(payload)
    stale_connections = []

    for socket in list(targets):
        try:
            await socket.send(encoded)
        except Exception:
            stale_connections.append(socket)

    for socket in stale_connections:
        await _remove_connection(socket)


async def handle_connection(websocket):
    CONNECTION_SUBSCRIPTIONS[websocket] = set()

    try:
        async for message in websocket:
            try:
                payload = json.loads(message)
            except json.JSONDecodeError as e:
                await websocket.send(json.dumps({"error": f"Invalid JSON: {e}"}))
                continue

            try:
                message_type = payload.get("type")

                if message_type == "statistic":
                    await asyncio.to_thread(
                        db.execute,
                        """
                        INSERT INTO gq_statistics
                        ("user",
                         "type",
                         amount,
                         enemy,
                         character,
                         weapon,
                         location,
                         status_effect,
                         visitation,
                         leaderboard)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            payload.get("user"),
                            payload.get("stat"),
                            payload.get("amount", 0),
                            payload.get("enemy", None),
                            payload.get("character", None),
                            payload.get("weapon", None),
                            payload.get("location", None),
                            payload.get("status_effect", None),
                            payload.get("visitation", None),
                            payload.get("leaderboard", None),
                        ),
                    )
                    response = {"ok": True}

                elif message_type == "subscribe_match":
                    match_id = _parse_match_id(payload.get("match_id"))
                    if match_id is None:
                        response = {"error": "match_id is required"}
                    else:
                        _add_subscription(websocket, match_id)
                        response = {"ok": True, "type": "subscribed", "match_id": match_id}

                elif message_type == "unsubscribe_match":
                    match_id = _parse_match_id(payload.get("match_id"))
                    if match_id is None:
                        response = {"error": "match_id is required"}
                    else:
                        subscribers = SUBSCRIBERS_BY_MATCH.get(match_id, set())
                        subscribers.discard(websocket)
                        if not subscribers:
                            SUBSCRIBERS_BY_MATCH.pop(match_id, None)

                        CONNECTION_SUBSCRIPTIONS.setdefault(websocket, set()).discard(match_id)
                        response = {"ok": True, "type": "unsubscribed", "match_id": match_id}

                elif message_type == "osu_user_refreshed":
                    user = payload.get("user", {})
                    match_user = payload.get("match_user")
                    sent_match_ids = payload.get("match_ids") or []
                    if payload.get("match_id") is not None:
                        sent_match_ids.append(payload.get("match_id"))

                    normalized_match_ids = []
                    for raw_match_id in sent_match_ids:
                        match_id = _parse_match_id(raw_match_id)
                        if match_id is not None:
                            normalized_match_ids.append(match_id)

                    normalized_match_ids = list(dict.fromkeys(normalized_match_ids))

                    outbound = {
                        "type": "osu_user_refreshed",
                        "match_id": payload.get("match_id"),
                        "match_ids": normalized_match_ids,
                        "user": user,
                        "match_user": match_user,
                    }
                    await _broadcast_to_match_ids(normalized_match_ids, outbound)

                    response = {"ok": True, "delivered_to_matches": normalized_match_ids}

                else:
                    response = {"error": "Unsupported type"}

            except Exception:
                response = {"error": "Invalid request"}

            await websocket.send(json.dumps(response))

    finally:
        await _remove_connection(websocket)


async def main():
    async with websockets.serve(
        handle_connection,
        HOST,
        PORT,
        process_request=health_check,
    ):
        print(f"WebSocket server listening on ws://{HOST}:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
