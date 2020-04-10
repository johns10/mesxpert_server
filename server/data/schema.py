import graphene
from graphene import relay
from data.data import get_opc_server, get_tag

class IntegrationClientSchema(graphene.ObjectType):
    id = graphene.String()
    runtime_id = graphene.Int()
    ip = graphene.String()
    name = graphene.String()

class TagSchema(graphene.ObjectType):

    class Meta:
        interfaces = (relay.Node,)

    namespace_index = graphene.Int()
    node_class = graphene.String()
    variant_type = graphene.String()
    node_id = graphene.String()

    @classmethod
    def get_node(cls, info):
        return []

class TagSchemaConnection(relay.Connection):
    class Meta:
        node = TagSchema

class OpcServerSchema(graphene.ObjectType):

    class Meta:
        interfaces = (relay.Node,)

    server_id = graphene.Int()
    name = graphene.String()
    runtime_id = graphene.String()
    url = graphene.String()
    tags = relay.ConnectionField(
        TagSchemaConnection,
        description="Tags on the server."
    )

    def resolve_tags(self, info):
        opc_servers = info.context.get('opc_servers')
        tags = []
        print("Resolving Tags")
        tag_schema = TagSchema(
            namespace_index=2,
            node_class='test2',
            variant_type='test2',
            node_id='test2'
        )
        tags.append(tag_schema)
        return tags

    @classmethod
    def get_node(cls, info):
        return get_opc_server(id)
