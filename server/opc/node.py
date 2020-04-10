import logging

from typing import Coroutine
from opcua import Node as OpcNode, ua
from anytree import Node as AnyTreeNode
from anytree.exporter import DictExporter

class Node(OpcNode, AnyTreeNode):
    def __init__(self, server, nodeid, name, parent, server_id, **kwargs):
        OpcNode.__init__(self, server=server, nodeid=nodeid)
        AnyTreeNode.__init__(self, name=name, parent=parent, **kwargs)
        self.namespace_index = self.nodeid.NamespaceIndex
        self.node_class = None
        self.variant_type = None
        self.string_nodeid = self.nodeid.to_string()
        self.node_id = self.nodeid.to_string()
        self.server_id = server_id
        self.web_client_ids = set()
        self.handle = None

    def get_children(self,
                     refs=ua.ObjectIds.HierarchicalReferences,
                     nodeclassmask=ua.NodeClass.Unspecified) -> Coroutine:
        return self.get_referenced_nodes(refs,
                                         ua.BrowseDirection.Forward,
                                         nodeclassmask)

    async def get_referenced_nodes(self,
                                   refs=ua.ObjectIds.References,
                                   direction=ua.BrowseDirection.Both,
                                   nodeclassmask=ua.NodeClass.Unspecified,
                                   includesubtypes=True):
        references = await self.get_references(
            refs,
            direction,
            nodeclassmask,
            includesubtypes
        )
        nodes = []
        for desc in references:
            node = Node(
                server=self.server,
                nodeid=desc.NodeId,
                name='',
                parent=self,
                server_id=self.server_id
            )
            node.name = (await node.get_display_name()).Text
            node.node_class = (await self.get_node_class())
            if node.node_class != ua.NodeClass.Variable:
                node.variant_type = None
            else:
                try:
                    node.variant_type = (await node.get_data_type_as_variant_type()).value
                except ua.UaError:
                    logging.warning('Node Variable Type could not be determined for %r', node)
                    node.variant_type = None
            nodes.append(node)
        return nodes

    async def exportTree(self):
        properties = set([
            'server_id',
            'name',
            'string_nodeid',
            'namespace_index',
            'node_class',
            'variant_type',
            'node_id',
            'permanent_id'
        ])
        logging.info(properties)
        exporter = DictExporter(
            attriter=lambda attrs: [(k, v) for k, v in attrs if k in properties]
        )
        return exporter.export(self)
