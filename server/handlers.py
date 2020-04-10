import logging
import json
from uuid import uuid4
from graphql_ws.aiohttp import AiohttpSubscriptionServer
from aiohttp import web, WSMsgType
from graphql import format_error

from modules.template import render_graphiql
from modules.webclient import Client
#from callbacks import api_commands_consumer, opc_tag_changes
#from commands.commands import CommandRunner
from data.query import schema

subscription_server = AiohttpSubscriptionServer(schema)

class Web(object):

    def configure(self, app):
        router = app.router
        router.add_route(
            'GET',
            '/graphiql',
            self.graphiql_view,
            name='graphiql_view'
        )
        router.add_route(
            'GET',
            '/graphql',
            self.graphql_view,
            name='graphql_view'
        )
        router.add_route(
            'POST',
            '/graphql',
            self.graphql_view,
            name='graphql_view'
        )
        router.add_route(
            'GET',
            '/ws',
            self.websocket_handler,
            name='websocket_handler'
        )
        router.add_route(
            'GET',
            '/subscriptions',
            self.subscriptions,
            name='subscriptions'
        )

    async def websocket_handler(self, request):
        """Handles websocket connections and messages"""
        ws = web.WebSocketResponse()
        client_id = request.cookies.get('client_id')
        if client_id is None:
            client_id = uuid4()
            logging.info("Client has no ID.  Setting Id to %s", client_id)
            ws.set_cookie('client_id', client_id)
        logging.info("%s connected", client_id)
        client = Client(
            client_id=client_id,
            ws=ws
        )
        client.connect()
        request.app['websocket_clients'][client_id] = client
        await ws.prepare(request)

        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                logging.info(msg.data)
                if msg.data == 'close':
                    await ws.close()
                elif msg.data == 'Connected':
                    logging.info("received connected")
                    message = 'Connected'
                    await ws.send_str(message)
                elif msg.json()['message_type'] == 'command':
                    message = msg.json()
                    logging.debug("Received %s from client", message)
                    command_type = message['command_type']
                    arguments = message['message']['arguments']
                    logging.debug("Passing arguments %s", arguments)
                    """await client_command_runner.execute(
                        command_type=command_type,
                        command=arguments
                    )"""
                elif msg.json()['message_type'] == 'event':
                    logging.info("Received event from client: %s",
                                 msg.json()['message'])
                    await ws.send_str(msg.json)
                else:
                    logging.info("received %s from client", msg)
                    await ws.send_str(msg.data + '/answer')
            elif msg.type == WSMsgType.ERROR:
                print('ws connection closed with exception %s' %
                      ws.exception())

        print('websocket connection closed')

        return ws

    async def graphql_view(self, request):
        payload = await request.json()
        response = await schema.execute(
            payload.get('query', ''),
            return_promise=True,
            context={'opc_servers': request.app['opc_servers']}
        )
        data = {}
        if response.errors:
            data['errors'] = [format_error(e) for e in response.errors]
        if response.data:
            data['data'] = response.data
        jsondata = json.dumps(data,)
        return web.Response(text=jsondata, headers={'Content-Type': 'application/json'})


    async def graphiql_view(self, request):
        """View for graphql explorer"""
        return web.Response(text=render_graphiql(), headers={'Content-Type': 'text/html'})

    async def subscriptions(self, request):
        """Subscription handler for graphql subscriptions"""
        ws = web.WebSocketResponse(protocols=('graphql-ws',))
        await ws.prepare(request)

        await subscription_server.handle(ws)
        return ws
