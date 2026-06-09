import http
import asyncio
import websockets
import json
import os
import dotenv
import logging
from PSQLConnector.connector import PSQLConnection as db

from opentelemetry import trace, metrics
from opentelemetry import _logs
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

from prometheus_client import start_http_server
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader

dotenv.load_dotenv()

start_http_server(port=8000, addr="0.0.0.0")
prometheus_reader = PrometheusMetricReader()
meter_provider = MeterProvider(metric_readers=[prometheus_reader])
metrics.set_meter_provider(meter_provider)

logger_provider = LoggerProvider()
_logs.set_logger_provider(logger_provider)
otel_log_exporter = OTLPLogExporter()
logger_provider.add_log_record_processor(BatchLogRecordProcessor(otel_log_exporter))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger().addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))
logger = logging.getLogger("websocket_server")

tracer = trace.get_tracer("gentry.ws")
meter = metrics.get_meter("gentry.ws")

active_connections = meter.create_up_down_counter(
    "ws_connections_active",
    description="Number of currently active websocket connections"
)
messages_processed = meter.create_counter(
    "ws_messages_processed",
    description="Total number of websocket messages handled"
)


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8765))

logger.info(f"Connecting to database at {os.environ.get('DB_HOSTNAME')}...")
try:
    db.connect(
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        host=os.environ.get("DB_HOSTNAME"),
        database=os.environ.get("DB"),
    )
    logger.info("Database connection established.")
except Exception as e:
    logger.error(f"Failed to connect to database: {e}")

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
    logger.debug(f"Added subscription for match {match_id}. Socket: {id(websocket)}")


async def _remove_connection(websocket):
    subscribed = CONNECTION_SUBSCRIPTIONS.pop(websocket, set())
    for match_id in subscribed:
        subscribers = SUBSCRIBERS_BY_MATCH.get(match_id)
        if not subscribers:
            continue

        subscribers.discard(websocket)
        if not subscribers:
            SUBSCRIBERS_BY_MATCH.pop(match_id, None)
    logger.info(f"Connection closed and cleaned up. Socket: {id(websocket)}")


async def _broadcast_to_match_ids(match_ids, payload):
    if not match_ids:
        return

    targets = set()
    for match_id in match_ids:
        targets.update(SUBSCRIBERS_BY_MATCH.get(match_id, set()))

    if not targets:
        logger.debug(f"No targets found for broadcast to matches: {match_ids}")
        return

    encoded = json.dumps(payload)
    stale_connections = []

    for socket in list(targets):
        try:
            await socket.send(encoded)
        except Exception as e:
            logger.warning(f"Failed to send to socket {id(socket)}: {e}")
            stale_connections.append(socket)

    for socket in stale_connections:
        await _remove_connection(socket)


async def handle_connection(websocket):
    addr = websocket.remote_address
    logger.info(f"New connection from {addr}")
    CONNECTION_SUBSCRIPTIONS[websocket] = set()
    active_connections.add(1)

    try:
        async for message in websocket:
            messages_processed.add(1)

            with tracer.start_as_current_span("process_ws_message") as span:
                try:
                    payload = json.loads(message)
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON received from {addr}: {e}")
                    await websocket.send(json.dumps({"error": f"Invalid JSON: {e}"}))
                    span.record_exception(e)
                    continue

                try:
                    message_type = payload.get("type")
                    span.set_attribute("message.type", str(message_type))

                    if message_type == "statistic":
                        logger.info(f"Recording statistic: {payload.get('stat')} for user {payload.get('user')}")
                        await asyncio.to_thread(
                            db.execute,
                            """
                            INSERT INTO gq_statistics
                            ("user", "type", amount, enemy, character, weapon, location, status_effect, visitation, leaderboard)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                payload.get("user"),
                                payload.get("stat"),
                                payload.get("amount", 0),
                                payload.get("enemy"),
                                payload.get("character"),
                                payload.get("weapon"),
                                payload.get("location"),
                                payload.get("status_effect"),
                                payload.get("visitation"),
                                payload.get("leaderboard"),
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

                        normalized_match_ids = list(dict.fromkeys(
                            m for m in (_parse_match_id(x) for x in sent_match_ids) if m is not None
                        ))

                        outbound = {
                            "type": "osu_user_refreshed",
                            "match_id": payload.get("match_id"),
                            "match_ids": normalized_match_ids,
                            "user": user,
                            "match_user": match_user,
                        }
                        logger.info(f"Broadcasting osu_user_refreshed for user {user.get('username')} to matches {normalized_match_ids}")
                        await _broadcast_to_match_ids(normalized_match_ids, outbound)
                        response = {"ok": True, "delivered_to_matches": normalized_match_ids}

                    else:
                        logger.warning(f"Unsupported message type received: {message_type}")
                        response = {"error": "Unsupported type"}

                except Exception as e:
                    logger.exception(f"Unexpected error processing message: {e}")
                    span.record_exception(e)
                    response = {"error": "Invalid request"}

                await websocket.send(json.dumps(response))

    finally:
        active_connections.add(-1)
        await _remove_connection(websocket)


async def main():
    async with websockets.serve(
            handle_connection,
            HOST,
            PORT,
            process_request=health_check,
            ping_interval=20,
            ping_timeout=20,
    ):
        logger.info(f"WebSocket server started on ws://{HOST}:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user.")