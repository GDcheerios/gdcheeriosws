from websockets.server import ServerConnection

from user import User


class Connection:
    websocket: ServerConnection
    """
    The websocket of this connection.
    """

    user: User | None
    """
    The user of this connection.
    
    Will be None if the connection is not authenticated.
    """

    subscriptions: dict
    """
    The subscriptions of this connection.
    """

    def __init__(self, websocket):
        self.websocket = websocket
        self.user = None
        self.subscriptions = {
            "osu_matches": set(),
            "chats": set(),
        }

    def authenticate(self, id: int):
        self.user = User(id)

    def subscribe(self, channel: str, state: str | int):
        if channel not in self.subscriptions:
            return

        self.subscriptions[channel].add(state)
