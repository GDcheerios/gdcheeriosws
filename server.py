from connection import Connection

from websockets.server import ServerConnection


class Server:
    connections: list[Connection]
    """
    The connections of the server
    """

    def __init__(self):
        self.connections = []

    def add_connection(self, websocket: ServerConnection):
        self.connections.append(Connection(websocket))

    def remove_connection(self, websocket: ServerConnection):
        self.connections.remove(Connection(websocket))

    def get_connection(self, websocket: ServerConnection) -> Connection:
        return self.connections[self.connections.index(Connection(websocket))]

    def process_message(self, payload: dict):
        if "type" not in payload:
            return

        if

