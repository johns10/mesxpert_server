import logging
import asyncio
from typing import Coroutine
from anytree import Node as AnyTreeNode, RenderTree, search
from anytree.exporter import DictExporter
import yaml
import json

from opcua import Client, ua

from opcserver import OpcServer
from node import Node
from ..commands.command import CommandRunner, PublishTags, PublishClient, PublishServers

class OpcClient(AnyTreeNode):
    def __init__(self, name, permanent_id, ip, server_config, queue_host):
        AnyTreeNode.__init__(
            self,
            name=name,
            parent=None
        )
        self.permanent_id = permanent_id
        self.ip = ip
        self.runtime_id = id(self)
        self.server_config = server_config
        self.queue_host = queue_host
        self.servers = {}

    def __get__(self, instance, owner):
        print(self.servers)

    def __str__(self):
        return self.name

    def dict(self):
        return {
            'id': self.permanent_id,
            'runtime_id': self.runtime_id,
            'ip': self.ip,
            'name': self.name
        }

    def get_integration_commands_queue(self):
        """A wrapper function that gets the integration commands queue objects
        from the self.queue_host value"""
        return {
            'channel': self.queue_host.exchanges['opc'].queues['commands'].channel,
            'exchange_name': self.queue_host.exchanges['opc'].name,
            'routing_key': self.queue_host.exchanges['opc'].queues['commands'].routing_key
        }

    def get_api_commands_queue(self):
        """Wrapper function that gets the api commands queue object from the
        self.queue_host value"""
        return {
            'channel': self.queue_host.exchanges['opc'].queues['events'].channel,
            'exchange_name': self.queue_host.exchanges['opc'].name,
            'routing_key': self.queue_host.exchanges['opc'].queues['events'].routing_key
        }

    def get_tag_change_queue(self):
        """Wrapper function that gets the tag change queue object from the
        self.queue_host"""
        return{
            'channel': self.queue_host.exchanges['opc'].queues['tag_changes'].channel,
            'exchange_name': self.queue_host.exchanges['opc'].name,
            'routing_key': self.queue_host.exchanges['opc'].queues['tag_changes'].routing_key
        }

    async def configure(self):
        for server_name, config in self.server_config.items():
            try:
                permanent_id = int(config['permanent_id'])
            except:
                permanent_id = ''
            try:
                host = config['host']
            except:
                host = ''
            try:
                port = str(config['port'])
            except:
                port = ''
            try:
                endpoint = config['endpoint']
            except:
                endpoint = ''
            url= 'opc.tcp://'+ host + ":" + port + endpoint
            server = OpcServer(
                name=config['name'],
                url=url,
                parent=self,
                permanent_id=permanent_id
            )
            self.servers[server.permanent_id] = server

    async def connect(self):
        for server in self.servers.values():
            logging.info("Connecting to server: %s", server.name)
            await server.connect()
            logging.info("Connected to server: %s", server.name)

    async def send_client_data(self, command_id, web_client_ids, payload):
        api_commands_queue = self.get_api_commands_queue()
        args = {
            'command_id': command_id,
            'web_client_ids': web_client_ids,
            'client': payload
        }
        command = PublishClient()
        message = await command.prepare_message(**args)
        await api_commands_queue['channel'].publish(
            payload=json.dumps(message, ensure_ascii=True),
            exchange_name=api_commands_queue['exchange_name'],
            routing_key=api_commands_queue['routing_key']
        )

    async def get_server_data(self, command_id, web_client_ids, integration_client_id, server_ids):
        payload = {}
        if not server_ids:
            logging.info("Received no Server ID's. Populating Server ID's.")
            server_ids = []
            for server_id in self.servers.keys():
                server_ids.append(server_id)
        logging.info(
            "Sending server information for %s",
            server_ids
        )
        for server_id, server in self.servers.items():
            if server_id in server_ids:
                payload[server_id] = await server.dict()

        logging.info("Servers: %s", payload)
        await self.send_server_data(
            command_id=command_id,
            web_client_ids=web_client_ids,
            integration_client_id=integration_client_id,
            payload=payload
        )

    async def send_server_data(self, command_id, web_client_ids, integration_client_id, payload):
        api_commands_queue = self.get_api_commands_queue()
        args = {
            'command_id': command_id,
            'web_client_ids': web_client_ids,
            'integration_client_id': integration_client_id,
            'servers': payload
        }
        command = PublishServers()
        message = await command.prepare_message(**args)
        await api_commands_queue['channel'].publish(
            payload = json.dumps(message, ensure_ascii=True),
            exchange_name=api_commands_queue['exchange_name'],
            routing_key=api_commands_queue['routing_key']
        )



    async def get_tags(self, command_id, server_id, web_client_ids):
        """Gets the tag tree from a server"""
        server = self.servers[server_id]
        tags = await server.get_tags()
        await self.send_tags(
            command_id=command_id,
            payload=tags,
            web_client_ids=web_client_ids,
            server_id=server_id,
            integration_client_id=server.parent.permanent_id
        )

    async def send_tags(self, command_id, payload, web_client_ids, server_id, integration_client_id):
        api_commands_queue = self.get_api_commands_queue()
        args = {
            'command_id': command_id,
            'web_client_ids': web_client_ids,
            'server_id': server_id,
            'integration_client_id': integration_client_id,
            'tag_tree': payload
        }
        command = PublishTags()
        message = await command.prepare_message(**args)
        await api_commands_queue['channel'].publish(
            payload=json.dumps(message, ensure_ascii=False),
            exchange_name=api_commands_queue['exchange_name'],
            routing_key=api_commands_queue['routing_key']
        )

    async def browse(self):
        """The function that starts a browse activity.  Establishes the root
        node, gets the objects node, and starts the browse_nodes function"""
        logging.info("Browsing Servers")
        for server_id, server in self.servers.items():
            server.uaclient.parent = server
            object_root = Node(
                name='',
                parent=server,
                server=server.uaclient,
                integration_client_id=self.permanent_id,
                server_id=server_id,
                nodeid=server.get_node(ua.TwoByteNodeId(ua.ObjectIds.ObjectsFolder))
            )
            object_root.name = (await object_root.get_display_name()).Text
            await server.browse_nodes(
                parent=server,
                node=object_root
            )

    async def search(self, attr, value):
        """Searches the servers tag tree for the specified attribute/value pair.
        Typically node ID."""
        logging.info("Searching %s for %s", attr, value)
        node = search.find_by_attr(self, name=attr, value=value)
        logging.info("node: %s", node)
        return node

    async def renderTree(self):
        logging.info("rendering %s", self)
        for pre, fill, node in RenderTree(self):
            print("%s%s" % (pre, node.name))

    async def exportTree(self):
        properties = set([
            'name',
            'string_nodeid',
            'namespace_index',
            'node_class',
            'variant_type',
            'id',
            'server_id'
        ])
        logging.info(properties)
        exporter = DictExporter(attriter=lambda attrs: [(k, v) for k, v in attrs if k in properties])
        return exporter.export(self)
