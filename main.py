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


async def health_check(path, request_headers):
    if path == "/":
        return http.HTTPStatus.OK, [], b"OK\n"
    return None


async def handle_connection(websocket):
    async for message in websocket:
        print("new message")
        try:
            payload = json.loads(message)
        except json.JSONDecodeError as e:
            print(e)
            await websocket.send(json.dumps({"error": f"Invalid JSON: {e}"}))
            continue

        try:
            if payload.get("type") == "statistic":
                print(payload)
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
            else:
                response = {"error": "Unsupported type"}
        except Exception as e:
            print(e)
            response = {"error": "Invalid request"}

        await websocket.send(json.dumps(response))


async def main():
    async with websockets.serve(handle_connection, HOST, PORT, process_request=health_check):
        print(f"WebSocket server listening on ws://{HOST}:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
