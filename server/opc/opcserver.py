import logging
import asyncio
import json
from anytree import Node as AnyTreeNode, RenderTree, search
from anytree.exporter import DictExporter
from opcua import Client, ua

from commands.commands import PublishSubscription, UnpublishSubscription
from opc.subscription import OpcSubscription
from opc.node import Node

#from command import (SubscribeTag)

class OpcServer(Client, AnyTreeNode):
    def __init__(self, url, name, server_id):
        Client.__init__(self, url)
        AnyTreeNode.__init__(self, name=name)
        self.runtime_id = id(self.uaclient)
        self.id = server_id
        self.url = url
        self.subscriptions = {}

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    async def dict(self):
        """serializable_subscriptions = self.subscriptions.copy()
        for subscription in serializable_subscriptions.values():
            logging.info(subscription)
            try:
                del subscription['subscription']
                del subscription['handle']
            except:
                pass
        logging.info("Serializable subscription %s", serializable_subscriptions)"""
        return {
            'name': self.name,
            'url': self.url,
            'runtime_id': self.runtime_id,
            'id': self.id
        }

    async def reconnect(self):
        try:
            while not self.parent.loop.is_running():
                await self.connect()

        finally:
            await self.disconnect()

        await asyncio.sleep(3)
        asyncio.ensure_future(self.reconnect())

    async def get_tags(self):
        tags = []
        for tag in self.children:
            tag_tree = await tag.exportTree()
            tags.append(tag_tree)
        # tags = await self.exportTree()
        logging.debug("Returning tags from server: {0}".format(tags))
        return tags

    async def browse(self):
        object_root = Node(
            name='',
            parent=self,
            server=self.uaclient,
            server_id=self.id,
            nodeid=self.get_node(
                ua.TwoByteNodeId(ua.ObjectIds.ObjectsFolder)
            )
        )
        object_root.name = (await object_root.get_display_name()).Text
        await self.browse_nodes(
            parent=self,
            node=object_root
        )


    async def browse_nodes(self, parent, node):
        """
        Build a nested node tree dict by recursion (filtered by OPC UA objects
        and variables).
        """
        node_class = await node.get_node_class()
        if node_class != ua.NodeClass.Variable:
            var_type = None
        else:
            try:
                var_type = (await node.get_data_type_as_variant_type()).value
            except ua.UaError:
                logging.warning('Node Variable Type could not be determined for %r', node)
                var_type = None
        for child in await node.get_children():
            if (await child.get_node_class() in [ua.NodeClass.Object,
                    ua.NodeClass.Variable]):
                logging.debug("Child is {0}, parent is {1}".format((
                    await child.get_display_name()).Text,
                    node.name))
                await self.browse_nodes(
                    parent=node,
                    node=child)

    async def subscribe(self, command_id, node, web_client_ids, subscription_interval, callback):
        """Subscribe to tag changes for a specified tag to a specified
        workstation namespace/channel, at the specified subscription interval.
        Typically invoked when a web client requests a subscription to a tag for
        display on a page.  Function builds a dictionary of which clients are
        subscribed to which tags, a dictionary of handlers, subscriptions and
        handles for each tag so we can manage them later.
        :param opc_identifier: the unique OPC Identifier for the tag to subscribe
        to
        :param opcNamespace: the OPC namespace of the tag to subscribe to
        :param session_id: the namespace of the websockets client we're sending
        info to
        :param interval: the subscription interval (in ms) that we want the
        client to send us updates
        """
        logging.info(
            "Processing subscribtion to %s",
            node.name
        )
        for web_client_id in web_client_ids:
            exists = await self.subscription_exists(node)
            subscribed = await self.client_subscribed(node, web_client_id)

            if not exists:
                node.web_client_ids.add(web_client_id)
                handler = callback
                try:
                    subscription = await self.create_subscription(
                        subscription_interval,
                        handler
                    )
                    handle = await subscription.subscribe_data_change(node)
                    self.subscriptions[node.node_id] = {
                        'subscription': subscription,
                        'handle': handle
                    }
                    logging.info("Subscription Succeeded")
                    create_subscription = True
                except Exception as exception:
                    logging.info("Subscription failed because of %s", exception)
                    create_subscription = False
            elif exists and not subscribed:
                node.web_client_ids.add(web_client_id)
                create_subscription = True
            elif exists and subscribed:
                create_subscription = True
            if create_subscription is True:
                await self.send_subscription(
                    command_id=command_id,
                    web_client_ids=web_client_ids,
                    node=node
                )

    async def send_subscription(self, command_id, web_client_ids, node):
        """Sends a PublishSubscript command out to the API server on a
        sucessful subscription"""
        api_commands_queue = self.parent.get_api_commands_queue()
        args = {
            'command_id': command_id,
            'web_client_ids': web_client_ids,
            'server_id': self.id,
            'node_id': node.node_id
        }
        command = PublishSubscription()
        await command.set_args(**args)
        await command.validate()
        message = await command.dict()
        logging.info(
            "Subscription JSON: %s",
            json.dumps(message, ensure_ascii=True)
        )
        await api_commands_queue['channel'].publish(
            payload=json.dumps(message, ensure_ascii=True),
            exchange_name=api_commands_queue['exchange_name'],
            routing_key=api_commands_queue['routing_key']
        )

    async def subscription_exists(self, node):
        """Checks if a subscription exists on a given node"""
        if not node.web_client_ids:
            has_client_ids = False
        elif node.web_client_ids:
            has_client_ids = True
        if node.node_id in self.subscriptions:
            subscription_exists = True
        elif node.node_id not in self.subscriptions:
            subscription_exists = False
        return has_client_ids and subscription_exists

    async def client_subscribed(self, node, web_client_id):
        """Checks if a given web_client_id is already subscribed to a given
        node"""
        if web_client_id in node.web_client_ids:
            logging.info(
                "%s is subscribed and publishing to %s",
                node.name,
                web_client_id
            )
            subscribed = True
        elif web_client_id not in node.web_client_ids:
            subscribed = False
        return subscribed

    async def unsubscribe(self, command_id, web_client_ids, node):
        logging.info("Unsubscribing from %s", node.node_id)
        for web_client_id in web_client_ids:
            exists = await self.subscription_exists(node)
            subscribed = await self.client_subscribed(node, web_client_id)

            if not exists:
                raise Exception('Subscription does not exist')
            elif exists:
                logging.info(
                    "Removing client from subscription.  Exists is %s, subscribed is %s",
                    exists,
                    subscribed
                )
                subscription = self.subscriptions[node.node_id]['subscription']
                handle = self.subscriptions[node.node_id]['handle']
                if subscribed:
                    node.web_client_ids.remove(web_client_id)
                else:
                    pass
                logging.info(
                    "Current subscribed clients: %s",
                    node.web_client_ids
                )
                await self.unpublish_subscription(
                    command_id=command_id,
                    web_client_ids=web_client_ids,
                    node=node
                )
            if (len(node.web_client_ids) == 0):
                logging.info(
                    'Removing subscription to %s',
                    node.node_id
                )
                await subscription.unsubscribe(handle)

    async def unpublish_subscription(self, command_id, web_client_ids, node):
        """Sends a PublishSubscript command out to the API server on a
        sucessful subscription"""
        api_commands_queue = self.parent.get_api_commands_queue()
        args = {
            'command_id': command_id,
            'web_client_ids': web_client_ids,
            'server_id': self.id,
            'node_id': node.node_id
        }
        command = UnpublishSubscription()
        await command.set_args(**args)
        await command.validate()
        message = await command.dict()
        await api_commands_queue['channel'].publish(
            payload=json.dumps(message, ensure_ascii=False),
            exchange_name=api_commands_queue['exchange_name'],
            routing_key=api_commands_queue['routing_key']
        )
        """            if client_id in node.clients:
            logging.info("{0} is subscribed to {1}".format(client_id,
                                                           string_nodeid))
            if (len(node.clients) > 1):
                logging.info("Multiple clients are subscribed to {0}".format(
                    string_nodeid))
                node.clients.remove(client_id)
                acknowledge = True
            elif (len(node.clients) == 1):
                await subscription.unsubscribe(handle)
                await subscription.delete()
                node.clients.remove(client_id)
                return True
            elif (len(node.clients) == 0):
                logging.info("There are no registered subscriptions on {0}".format(
                    string_nodeid))
                if (subscription is not None):
                    logging.info("There is a dangling subscription on {0}".format(
                        string_nodeid))
                    await subscription.unsubscribe(handle)
                    await subscription.delete()
                    node.clients.clear()
                    return True"""

    def getNodeFromNsAndId(self, namespace, opc_identifier):
        """Gets the node object from the OPC Server, based on the namespace and identifier
        :param namespace: the namespace of the node
        :param identifier: the identifier of the node (unique within the namespace)
        """
        nodeToFetch = ua.uatypes.NodeId(
            namespaceidx=namespace,
            identifier=opc_identifier
        )
        node = self.get_node(nodeToFetch)
        return node

    """async def publishOnMq(self, node, val, data, channel, exchange, queue):

        Publish a tag change on the channel passed in to the function.  This
        Function should generally publish to the tagChangeQueue.
        :param channel: the channel the
        :param node: an OPCUA node object passed in from the callback
        :param val: the data value resulting from the tag change
        :param data: not sure

        nodeName, namespace, opc_identifier = self.getNsAndIdFromNode(node)
        timestamp = data.monitored_item.Value.ServerTimestamp
        logging.info("Received data change notification on {0}.  Publishing
            notification to {1}.".format(nodeName,
            self.clientIndex[opc_identifier]))
        try:
            self.clientIndex[opc_identifier]
            for client in self.clientIndex[opc_identifier]:
                self.socketio.emit('message', {}, namespace=client)
        except:
            return 'cannot update tag value'"""

    async def disconnect(self):
        await self.disconnect()

    async def renderTree(self):
        #render the tree
        logging.info("rendering {0}".format(self))
        for pre, fill, node in RenderTree(self):
            print("%s%s" % (pre, node.name))

    async def exportTree(self):
        properties = set([
            'name',
            'string_nodeid',
            'namespace_index',
            'node_class',
            'variant_type',
            'node_id',
            'permanent_id',
            'runtime_id',
            'server_id',
            'integration_client_id',
            'subscriptions',
            'url '
        ])
        logging.info(properties)
        exporter = DictExporter(
            attriter=lambda attrs: [(k, v) for k, v in attrs if k in properties]
        )
        return exporter.export(self)

    async def search(self, attr, value):
        logging.info("Searching {0} for {1}".format(attr, value))
        node = search.find_by_attr(self, name=attr, value=value)
        logging.info("node: {0}".format(node))
        return node

    async def write_tag(self, namespace, id, value):
        node = self.get_node(id)
        await node.set_value(value)

    async def create_subscription(self, period, handler):
        """
        Create a subscription.
        returns a Subscription object which allow
        to subscribe to events or data on server
        handler argument is a class with data_change and/or event methods.
        period argument is either a publishing interval in milliseconds or a
        CreateSubscriptionParameters instance. The second option should be used,
        if the opcua-server has problems with the default options.
        These methods will be called when notfication from server are received.
        See example-client.py.
        Do not do expensive/slow or network operation from these methods
        since they are called directly from receiving thread. This is a design choice,
        start another thread if you need to do such a thing.
        """

        if isinstance(period, ua.CreateSubscriptionParameters):
            subscription = OpcSubscription(
                server=self.uaclient,
                params=period,
                handler=handler
            )
            await subscription.init()
            return subscription
        params = ua.CreateSubscriptionParameters()
        params.RequestedPublishingInterval = period
        params.RequestedLifetimeCount = 10000
        params.RequestedMaxKeepAliveCount = 3000
        params.MaxNotificationsPerPublish = 10000
        params.PublishingEnabled = True
        params.Priority = 0
        subscription = OpcSubscription(
            server=self.uaclient,
            params=params,
            handler=handler
        )
        await subscription.init()
        return subscription
