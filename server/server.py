import logging
import asyncio
import yaml
from aiohttp import web

from opc.opcserver import OpcServer
from handlers import Web

logging.basicConfig(format='%(levelname)s:%(message)s')

"""async def initialize_opc_servers(app):
    for opc_server in app['opc_servers'].values():
        await opc_server.connect()
        await opc_server.browse()"""

async def init(loop):
    app = web.Application()

    app['websocket_clients'] = {}
    app['opc_servers'] = {}

    with open('server_config.yml', 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
    for opc_server_config in cfg['opc servers'].values():
        id = opc_server_config['permanent_id']
        host = opc_server_config['host']
        port = opc_server_config['port']
        endpoint = opc_server_config['endpoint']
        opc_server = OpcServer(
            url='opc.tcp://' + host + ":" + str(port) + endpoint,
            name=opc_server_config['name'],
            server_id=id
        )
        await opc_server.connect()
        await opc_server.browse()
        logging.info(opc_server)
        app['opc_servers'][id] = opc_server

    web_handlers = Web()
    web_handlers.configure(app)
    return app

def main():
    """Main Function"""
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init(loop))
    #loop.run_forever()
    web.run_app(app, port=1000)

if __name__ == '__main__':
    main()
