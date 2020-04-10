import logging
import json
from importlib import import_module

from typing import NamedTuple
from collections import namedtuple

logging.basicConfig(level=logging.INFO)

class Argument (NamedTuple):
    """Named Tuple argument used to define arguments for commands."""
    type: type
    required: bool
    validator: object = None
    options: str = None

class CommandRunner():
    """Method used for running commands"""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        #self._populate(kwargs)
        self.command_runner = self

    async def execute(self, command_type, command, **kwargs):
        """Command executor method"""
        # user_id = getattr(self, 'user_id', 0)

        command_name, command_handler = await self._get_command_handler(command_type)
        await self._write_preflight_log(command_type, command)
        command_handler = await self._prepare_command_handler(command_handler, command)

        try:
            result = await command_handler.execute()
        except Exception as exception:
            await self._write_error_log(command_name, command_handler, exception)
            raise # -*- coding: utf-8 -*-
        else:
            await self._write_success_log(command_name, command_handler)

        return result

    async def _get_command_handler(self, command_type):
        """Resolve command module by name and import handler."""
        if isinstance(command_type, str):
            module_name = 'command'
            module = import_module(module_name)
            handler = getattr(module, command_type)
            return command_type, handler

    async def _prepare_command_handler(self, command_handler, command):
        command_handler = command_handler()

        await self._inject_dependencies(command_handler)

        args = {} if command is None else command
        await command_handler.set_args(**args)
        await command_handler.validate()
        return command_handler

    async def _inject_dependencies(self, command_handler):
        try:
            for dep in command_handler.requires:
                setattr(command_handler, dep, getattr(self, dep))
        except AttributeError:
            raise ValueError('Cannot provide requirements for action: {}'
                             .format(type(command_handler).__name__))

    async def _write_preflight_log(self, command_name, command):
        """Logs action name along with raw data submitted by user."""
        logging.info('Action submitted: [%s] [%s]', command_name,
                     json.dumps(command))

    async def _write_error_log(self, command_name, command_handler, e):
        logging.info('Action raised error: [%s] [%s] [%s]',
                     command_name, e, str(command_handler))

    async def _write_success_log(self, command_name, command_handler):
        logging.info('Action succeeded: [%s] [%s]',
                     command_name, str(command_handler))

class CommandHandler:
    """Base class for action handlers, which contain the logic for all the core use cases.
    Each action handler should be defined in its own file, exposed as `handler`, and executed
    by the ActionRunner.
    Attributes:
    - requires: list of objects that should be injected by ActionRunner
    - arguments: arguments that should be set before execute() is called
    """
    """def __init__(self):
        self.original_arguments = None
        self.arguments = None
        self._args = None
        self.requires = []
        self.arguments = {}"""
    requires = []
    arguments = {}

    async def prepare_message(self, **args):
        """Small function that prepares a message to send across the queue"""
        await self.set_args(**args)
        await self.validate()
        return await self.dict()

    async def validate(self):
        """Typically overridden by specific command"""
        pass

    async def set_args(self, **kwargs):
        """Validate and populate action arguments.
        Each action instance defines the arguments it accepts, including the name, value type,
        default value, etc (see Arg for all options). This method loops through provided arguments
        and validates against the definition of the Arg. If any args don't validate, an error is
        raised here. Otherwise, they are set on the action instance _args attribute as a namedtuple.
        """
        self.original_arguments = kwargs
        Args = namedtuple('Args', [k for k, v in self.arguments.items()])
        Args.__new__.__defaults__ = (None,) * len(self.arguments.items())

        valid = {}
        for k, arg in self.arguments.items():
            val = kwargs.get(k, None)
            if val is None and arg.required:
                raise Exception('{0} is required'.format(k))

            if arg.options and val not in arg.options:
                raise Exception('{0} provided for {1}.  Expected {2}'.format(
                    val,
                    k,
                    arg.options
                ))

            if callable(arg.validator):
                val = arg.validator(val, k)

            valid[k] = val

        self._args = Args(**valid)

    def sync_set_args(self, **kwargs):
        """This is a synchronous copy of the set_args function.  We only use This
        for datachange_notification in class TagChangeHandler because subscription
        callbacks must by synchronous for now.
        """
        self.original_arguments = kwargs
        Args = namedtuple('Args', [k for k, v in self.arguments.items()])
        Args.__new__.__defaults__ = (None,) * len(self.arguments.items())

        valid = {}
        for k, arg in self.arguments.items():
            val = kwargs.get(k, None)
            if val is None and arg.required:
                raise Exception('{0} is required'.format(k))

            if arg.options and val not in arg.options:
                raise Exception('{0} provided for {1}.  Expected {2}'.format(
                    val,
                    k,
                    arg.options
                ))

            if callable(arg.validator):
                val = arg.validator(val, k)

            valid[k] = val

        self._args = Args(**valid)

    async def dict(self):
        """Builds a dict of the arguments of the command for sending across
        the queue"""

        #pylint disable=no-member

        args = {}
        for item, value in self._args._asdict().items():
            args[item] = value

        message = {
            'message_type': 'command',
            'command_type': self.__class__.__name__,
            'message': {
                'arguments': args
            }
        }
        return message

    def sync_dict(self):
        """This is a copy of the dict function.  It's only used in the
        datachange_notification function in class TagChangeHandler, because
        subscription callbacks must be synchronous (for now)."""
        #pylint disable=no-member

        args = {}
        for item, value in self._args._asdict().items():
            args[item] = value

        message = {
            'message_type': 'command',
            'command_type': self.__class__.__name__,
            'message': {
                'arguments': args
            }
        }
        return message

class GetIntegrationClients(CommandHandler):
    """This command is issued from the web client to the api server.  The api
    sends a SendIntegrationClients command to each integration client to send
    back information about the client."""
    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'queue_host',
        'integration_clients'
    ]
    arguments = {
        'web_client_ids': Argument(list, required=True),
        'command_id': Argument(str, required=True),
        'integration_client_ids': Argument(list, required=False)
    }

    async def validate(self):
        pass

    async def execute(self):
        """if no integration client id's are specified in args, get all clients
        attached to the queue.  Basically just fires off a SendIntegrationClient
        command to each integration client.  The integration clients are
        initially defined in the config."""
        args = self._args

        channel = self.queue_host.exchanges['opc'].queues['commands'].channel
        exchange_name = self.queue_host.exchanges['opc'].name
        routing_key = self.queue_host.exchanges['opc'].queues['commands'].routing_key

        if not args.integration_client_ids:
            for integration_client_id in self.integration_clients.keys():
                args.integration_client_ids.append(integration_client_id)

        for integration_client_id in args.integration_client_ids:
            command_args = {
                'web_client_ids': args.web_client_ids,
                'command_id': args.command_id,
            }
            command = SendIntegrationClient()
            message = await command.prepare_message(**command_args)
            await channel.publish(
                payload=json.dumps(message, ensure_ascii=False),
                exchange_name=exchange_name,
                routing_key=routing_key + '_' + str(integration_client_id)
            )

class SendIntegrationClient(CommandHandler):
    """This command instructs the integration server to send information about
    itself, and it's attached servers/devices."""
    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'integration_client',
        'rethink_handler'
    ]
    arguments = {
        'web_client_ids': Argument(list, required=True),
        'command_id': Argument(str, required=False)
    }

    async def validate(self):
        pass

    async def execute(self):
        """payload = {}
        payload['arguments'] = await self.integration_client.get_client_data(
            web_client_ids=args.web_client_ids,
            command_id=args.command_id
        )"""
        payload = self.integration_client.dict()

        await self.rethink_handler.single_insert_update(
            db='mesxpert_view',
            table='integration_clients',
            payload=payload
        )

class PublishClient(CommandHandler):
    """This command instructs the API Server to publish client information to
    the specified web clients."""
    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'websocket_clients',
        'integration_clients'
    ]
    arguments = {
        'client': Argument(str, required=True),
        'web_client_ids': Argument(list, required=False),
        'command_id': Argument(str, required=False)
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        logging.debug(
            "Current Clients %s",
            self.integration_clients
        )

        for integration_client_id, integration_client in args.client.items():
            logging.debug(
                "Publishing Integration Client %s",
                self.integration_clients[int(integration_client_id)]
            )
            self.integration_clients[int(integration_client_id)] = integration_client

        logging.info(self.integration_clients)

        message = await self.dict()

        for client_id, client in self.websocket_clients.items():
            if client_id in args.web_client_ids:
                await client.ws.send_str(json.dumps(message))

class GetServers(CommandHandler):
    """This command is issued from the web client to the api server.  The api
    sends a SendServerData command to a specific integration client to send
    back information about the attached servers.  If the server is not
    specified, the client """
    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'queue_host',
        'integration_clients'
    ]
    arguments = {
        'web_client_ids': Argument(list, required=True),
        'command_id': Argument(str, required=True),
        'integration_client_id': Argument(list, required=True),
        'server_ids': Argument(list, required=False)
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        channel = self.queue_host.exchanges['opc'].queues['commands'].channel
        exchange_name = self.queue_host.exchanges['opc'].name
        routing_key = self.queue_host.exchanges['opc'].queues['commands'].routing_key

        integration_client = self.integration_clients[args.integration_client_id]
        logging.info(
            "Getting servers from Integration Client %s",
            integration_client
        )

        command_arguments = args._asdict()
        logging.info(
            "Sending arguments %s",
            command_arguments
        )

        command = SendServers()
        message = await command.prepare_message(**command_arguments)
        await channel.publish(
            payload=json.dumps(message, ensure_ascii=False),
            exchange_name=exchange_name,
            routing_key=routing_key + '_' + str(args.integration_client_id)
        )

class SendServers(CommandHandler):
    """This command is issued from the api server to the integration client.
    It instructs the integration client to send over information about it's
    attached servers."""
    # pylint: disable=no-member
    # args are defined dynamically

    requires = ['integration_client']
    arguments = {
        'web_client_ids': Argument(list, required=True),
        'command_id': Argument(str, required=False),
        'integration_client_id': Argument(list, required=True),
        'server_ids': Argument(list, required=False)

    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        await self.integration_client.get_server_data(
            command_id=args.command_id,
            web_client_ids=args.web_client_ids,
            integration_client_id=args.integration_client_id,
            server_ids=args.server_ids
        )

class PublishServers(CommandHandler):
    """This command is issued from an integration client to the API server.  It
    instructs the API Server to send server information to specified web clients.
    """
    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'websocket_clients',
        'integration_clients'
    ]
    arguments = {
        'web_client_ids': Argument(list, required=True),
        'command_id': Argument(str, required=True),
        'integration_client_id': Argument(list, required=True),
        'servers': Argument(str, required=True)
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        logging.info(args.payload)

        integration_client = self.integration_clients[args.integration_client_id]

        logging.info(
            "Publishing servers for %s",
            integration_client
        )

        if not integration_client['children']:
            integration_client['children'] = {}

        for server_id, server in args.servers:
            if not integration_client['children'][server_id]:
                integration_client['children'][server_id] = server
            elif integration_client['children'][server_id]:
                old_server = integration_client['children'][server_id]
                try:
                    subscriptions = dict(old_server['subscriptions'])
                except:
                    logging.info(
                        "No Subscriptions exist on server %s",
                        server_id
                    )
                    subscriptions = {}
                try:
                    tag_tree = list(old_server['children'])
                except:
                    logging.info(
                        "Server %s has no children",
                        tag_tree
                    )
                    tag_tree = []
                integration_client['children'][server_id] = server
                new_server = integration_client['children'][server_id]
                new_server['subscriptions'] = subscriptions
                new_server['children'] = tag_tree







class GetTags(CommandHandler):
    """This is a command from the web client to the API server to get tags from
    the integration server."""
    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'queue_host',
        'integration_clients'
    ]
    arguments = {
        'integration_client_id': Argument(int, required=True),
        'server_id': Argument(int, required=True),
        'web_client_ids': Argument(list, required=False),
        'command_id': Argument(str, required=False)
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args
        channel = self.queue_host.exchanges['opc'].queues['commands'].channel
        exchange_name = self.queue_host.exchanges['opc'].name
        routing_key = self.queue_host.exchanges['opc'].queues['commands'].routing_key

        command = SendTags()
        command_args = {
            'command_id': args.command_id,
            'web_client_ids': args.web_client_ids,
            'server_id': args.server_id
        }
        message = await command.prepare_message(**command_args)
        await channel.publish(
            payload=json.dumps(message, ensure_ascii=False),
            exchange_name=exchange_name,
            routing_key=routing_key + '_' + str(args.integration_client_id)
        )

class SendTags(CommandHandler):

    # pylint: disable=no-member
    # args are defined dynamically

    requires = ['integration_client']
    arguments = {
        'server_id': Argument(int, required=True),
        'web_client_ids': Argument(list, required=False),
        'command_id': Argument(str, required=False)
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        await self.integration_client.get_tags(
            command_id=args.command_id,
            server_id=args.server_id,
            web_client_ids=args.web_client_ids
        )

class PublishTags(CommandHandler):

    # pylint: disable=no-member
    # args are defined dynamically

    requires = ['websocket_clients']
    arguments = {
        'command_id': Argument(str, required=True),
        'web_client_ids': Argument(list, required=True),
        'server_id': Argument(int, required=True),
        'integration_client_id': Argument(int, required=True),
        'tag_tree': Argument(str, required=True),
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        message = await self.dict()

        for client_id, client in self.websocket_clients.items():
            if client_id in args.web_client_ids:
                logging.info("Sending tag tree to %s", client_id)
                await client.ws.send_str(json.dumps(message))

class CreateSubscription(CommandHandler):

    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'queue_host',
        'integration_clients'
    ]
    arguments = {
        'web_client_ids': Argument(list, required=True),
        'command_id': Argument(str, required=True),
        'integration_client_id': Argument(int, required=True),
        'server_id': Argument(int, required=True),
        'node_id': Argument(str, required=True),
        'subscription_interval': Argument(int, required=False)
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        channel = self.queue_host.exchanges['opc'].queues['commands'].channel
        exchange_name = self.queue_host.exchanges['opc'].name
        routing_key = self.queue_host.exchanges['opc'].queues['commands'].routing_key

        command = SubscribeTag()
        message = await command.prepare_message(**args._asdict())
        await channel.publish(
            payload=json.dumps(message, ensure_ascii=False),
            exchange_name=exchange_name,
            routing_key=routing_key + '_' + str(args.integration_client_id)
        )

class SubscribeTag(CommandHandler):

    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'integration_client',
        'tag_change_queue',
        'subscription_callback',
        'api_commands_queue'
    ]
    arguments = {
        'command_id': Argument(str, required=True),
        'web_client_ids': Argument(list, required=False),
        'server_id': Argument(int, required=True),
        'node_id': Argument(str, required=True),
        'subscription_interval': Argument(int, required=True)
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        server = self.integration_client.servers[args.server_id]
        node = await server.search(
            attr="string_nodeid",
            value=args.node_id
        )
        await server.subscribe(
            command_id=args.command_id,
            web_client_ids=args.web_client_ids,
            subscription_interval=args.subscription_interval,
            node=node,
            callback=self.subscription_callback(
                queue=self.api_commands_queue
            )
        )

class PublishSubscription(CommandHandler):

    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'websocket_clients',
        'integration_clients'
    ]
    arguments = {
        'command_id': Argument(str, required=True),
        'web_client_ids': Argument(list, required=False),
        'node_id': Argument(str, required=True),
        'integration_client_id': Argument(int, required=True),
        'server_id': Argument(int, required=True)

    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args
        message = await self.dict()

        integration_client_id = args.integration_client_id
        server_id = str(args.server_id)

        logging.info(self.integration_clients[integration_client_id])

        logging.info(
            "Publishing a subscription on %s",
            self.integration_clients[integration_client_id]['children'] \
            [server_id]
        )
        server = self.integration_clients[integration_client_id]['children'] \
        [server_id]

        if args.node_id not in server['subscriptions']:
            server['subscriptions'][args.node_id] = {}
        else:
            pass

        subscription = server['subscriptions'][args.node_id]

        for client_id in args.web_client_ids:
            if client_id not in subscription:
                subscription[client_id] = self.websocket_clients[client_id]
            else:
                raise Exception(
                    "{0} already subscribed to {1}".format(
                        client_id,
                        args.node_id
                    ))

        for client_id, client in self.websocket_clients.items():
            if client_id in args.web_client_ids:
                await client.ws.send_str(json.dumps(message))

        logging.info("Current Subscriptions %s", server['subscriptions'])

class UnsubscribeTags(CommandHandler):

    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'integration_client'
    ]
    arguments = {
        'command_id': Argument(str, required=True),
        'web_client_ids': Argument(list, required=False),
        'server_id': Argument(int, required=True),
        'node_id': Argument(str, required=True)
    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args

        server = self.integration_client.servers[args.server_id]
        try:
            node = await server.search(
                attr="string_nodeid",
                value=args.node_id
            )
        except Exception as exception:
            logging.info(exception)
        await server.unsubscribe(
            command_id=args.command_id,
            web_client_ids=args.web_client_ids,
            node=node
        )

class UnpublishSubscription(CommandHandler):
    """This is a command that unpublishes subscriptions from the API server,
    and sends commands to the relevant webclients to unsubscribe their
    subscriptions."""

    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'websocket_clients',
        'subscriptions'
    ]
    arguments = {
        'command_id': Argument(str, required=True),
        'web_client_ids': Argument(list, required=False),
        'node_id': Argument(str, required=True),
        'client_id': Argument(int, required=True),
        'server_id': Argument(int, required=True)

    }

    async def validate(self):
        pass

    async def execute(self):
        args = self._args
        message = await self.dict()

        if args.node_id not in self.subscriptions:
            raise Exception('Subscription does not exist on API Server')
        else:
            subscription = self.subscriptions[args.node_id]

        for client_id in args.web_client_ids:
            if client_id in subscription:
                del subscription[client_id]

        for client_id, client in self.websocket_clients.items():
            if client_id in args.web_client_ids:
                await client.ws.send_str(json.dumps(message))

class PublishTagChange(CommandHandler):

    # pylint: disable=no-member
    # args are defined dynamically

    requires = [
        'websocket_clients',
        'integration_clients'
    ]
    arguments = {
        'command_id': Argument(str, required=True),
        'node_id': Argument(str, required=True),
        'server_id': Argument(int, required=True),
        'integration_client_id': Argument(int, required=True),
        'value': Argument(any, required=True),
        'server_timestamp': Argument(str, required=True),
        'source_timestamp': Argument(str, required=True)
    }

    def sync_validate(self):
        pass

    async def validate(self):
        pass

    async def execute(self):
        args = self._args
        integration_client_id = args.integration_client_id
        server_id = str(args.server_id)
        message = await self.dict()
        logging.info(message)
        logging.info(self.integration_clients)
        subscription = self.integration_clients \
            [integration_client_id]['children'][server_id]['subscriptions'] \
            [args.node_id]

        logging.info("Working with subscription %s", subscription)
        for web_client_id, web_client in subscription.items():
            logging.info(
                "Publishing tag change to client ID %s",
                web_client_id
            )
            await web_client.ws.send_str(json.dumps(message))
