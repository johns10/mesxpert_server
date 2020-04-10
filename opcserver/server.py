import asyncio
from opcua import ua, Server
from opcua.common.methods import uamethod


@uamethod
def func(parent, value):
    return value * 2


async def task(loop):
    # setup our server
    server = Server()
    await server.init()
    server.set_endpoint('opc.tcp://0.0.0.0:4840/freeopcua/server/') #4840
    # setup our own namespace, not really necessary but should as spec
    uri = 'http://examples.freeopcua.github.io'
    idx = await server.register_namespace(uri)
    # get Objects node, this is where we should put our nodes
    objects = server.get_objects_node()

    await objects.add_method(
        ua.NodeId("ServerMethod", 2), ua.QualifiedName('ServerMethod', 2),
        func, [ua.VariantType.Int64], [ua.VariantType.Int64]
    )

    # populating our address space
    myobj = await objects.add_object(idx, 'MyObject')
    myvar = await myobj.add_variable(idx, 'MyVariable', 6.7)
    await myvar.set_writable()  # Set MyVariable to be writable by clients

    # starting!
    async with server:
        count = 0
        while True:
            await asyncio.sleep(1)
            count += 0.1
            await myvar.set_value(count)


def main():
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.run_until_complete(task(loop))
    loop.close()


if __name__ == '__main__':
    main()
