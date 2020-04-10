import graphene
import asyncio
import logging

from data.schema import OpcServerSchema, TagSchema

class Query(graphene.ObjectType):
    opc_server = graphene.Field(
        OpcServerSchema,
        id=graphene.Int(),
        description='Returns a single OPC Server'
    )
    opc_servers = graphene.List(
        OpcServerSchema,
        description='Returns all OPC Servers'
    )
    """tags = graphene.List(
        TagSchema
    )"""

    def resolve_opc_servers(self, info):
        opc_servers = info.context.get('opc_servers')
        opc_servers_schema = []
        for opc_server in opc_servers.values():
            opc_server_schema = OpcServerSchema(
                runtime_id=str(opc_server.runtime_id),
                server_id=opc_server.id,
                name=opc_server.name,
                url=opc_server.url
            )
            opc_servers_schema.append(opc_server_schema)
        return opc_servers_schema

    def resolve_opc_server(self, info, id):
        opc_servers = info.context.get('opc_servers')
        opc_server_schema = OpcServerSchema(
            runtime_id=str(opc_servers[id].runtime_id),
            server_id=opc_servers[id].id,
            name=opc_servers[id].name,
            url=opc_servers[id].url
        )
        return opc_server_schema

    def resolve_tags(self, info, id):
        opc_servers = info.context.get('opc_servers')
        #tags = await opc_servers[1].get_tags()
        tags = []
        #print("Resolving Tags")
        tag_schema = TagSchema(
            namespace_index=1,
            node_class='test',
            variant_type='test',
            node_id='test'
        )
        tags.append(tag_schema)
        return tags



schema = graphene.Schema(query=Query)
