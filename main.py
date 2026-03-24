import asyncio
import json
import websockets
import os
import dotenv
from PSQLConnector.connector import PSQLConnection as db

HOST = "0.0.0.0"
PORT = 8765
dotenv.load_dotenv()

print(os.environ.get("DB_HOSTNAME"))
db.connect(
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    host=os.environ.get("DB_HOSTNAME"),
    database=os.environ.get("DB"),
)


async def handle_connection(websocket):
    global data
    async for message in websocket:
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            payload = {"error": f"Invalid JSON: {e}"}
            await websocket.send(json.dumps(payload))

    payload = data
    try:
        if payload.get("type") == "statistic":
            db.execute(
                """
                INSERT INTO statistics
                (user,
                 type,
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
                params=(
                    payload.get("user"),
                    payload.get("stat"),
                    payload.get("amount"),
                    payload.get("enemy"),
                    payload.get("character"),
                    payload.get("weapon"),
                    payload.get("location"),
                    payload.get("status_effect"),
                    payload.get("visitation"),
                    payload.get("leaderboard")
                )
            )

    except:
        payload = {"error": "Invalid request"}

    await websocket.send(json.dumps(payload))


async def main():
    async with websockets.serve(handle_connection, HOST, PORT):
        print(f"WebSocket server listening on ws://{HOST}:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
