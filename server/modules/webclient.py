class Client:
    def __init__(self, client_id, ws):
        self.client_id = client_id
        self.subscriptions = {}
        self.connected = False
        self.ws = ws

    def connect(self):
        self.connected = True

    def disconnect(self):
        self.connected = False

    def add_subscription(self):
        pass
