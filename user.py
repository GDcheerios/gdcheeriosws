from PSQLConnector.connector import PSQLConnection as db


class User:
    id: int
    """The identifier for this user"""

    sources: list[str]
    """
    List of sources for this user.
    
    example:
        web:osu match 12
        gq:at Brayden's house
    """

    def __init__(self, id: int):
        self.id = id
        self.sources = []

    def add_source(self, source):
        self.sources.append(source)

    def remove_source(self, source):
        self.sources.remove(source)

    def submit_statistic(self, payload):
        db.execute("INSERT INTO statistics (user_id, statistic) VALUES (%s, %s)", (self.id, statistic))

    def __repr__(self):
        return f"<User id={self.id} sources={self.sources}>"
