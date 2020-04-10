from opcua.common.subscription import Subscription
import logging

class OpcSubscription(Subscription):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def dict(self):
        logging.info(
            "Monitored items %s",
            self._monitored_items
        )
