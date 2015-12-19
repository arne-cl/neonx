
# -*- coding: utf-8 -*-

import json

import networkx as nx
import requests

__all__ = ['write_to_neo', 'get_neo_graph']


JSON_CONTENT_TYPE = 'application/json; charset=utf-8'
HEADERS = {'content-type': JSON_CONTENT_TYPE}


def get_node(node_id, properties):
    """reformats a NetworkX node for `generate_data()`.

    :param node_id: the index of a NetworkX node
    :param properties: a dictionary of node attributes
    :rtype: a dictionary representing a Neo4j POST request
    """
    return {"method": "POST",
            "to": "/node",
            "id": node_id,
            "body": properties}


def get_relationship(from_id, to_id, rel_name, properties):
    """reformats a NetworkX edge for `generate_data()`.

    :param from_id: the ID of a NetworkX source node
    :param to_id: the ID of a NetworkX target node
    :param rel_name: string that describes the relationship between the
        two nodes
    :param properties: a dictionary of edge attributes
    :rtype: a dictionary representing a Neo4j POST request
    """
    body = {"to": "{{{0}}}".format(to_id), "type": rel_name,
            "data": properties}

    return {"method": "POST",
            "to": "{{{0}}}/relationships".format(from_id),
            "body": body}


def get_label(i, label):
    """adds a label to the given (Neo4j) node.

    :param i: the index of a NetworkX node
    :param label: the label to be added to the node
    :rtype: a dictionary representing a Neo4j POST request
    """
    return {"method": "POST",
            "to": "{{{0}}}/labels".format(i),
            "body": label}


def generate_data(graph, edge_rel_name=None, label=None, encoder=None,
                  edge_rel_key=None):
    """converts a NetworkX graph into a format that can be uploaded to
    Neo4j using a single HTTP POST request.

    If `edge_rel_name` is not present, then `edge_rel_key` should be provided.

    :rtype: a JSON encoded string for `Neo4j batch operations \
    <http://docs.neo4j.org/chunked/stable/rest-api-batch-ops.html>_`.

    :param graph: A NetworkX Graph or a DiGraph
    :param optional edge_rel_name: string that describes the relationship
        between the two nodes
    :param label: an optional label to be added to all nodes
    :param encoder: a JSONEncoder object
    :param optional edge_rel_key: Key in edge attributes to use as edge label.
    """

    if edge_rel_name is None and edge_rel_key is None:
        raise ValueError(
            'Must provide either `edge_rel_name` or `edge_rel_key`')

    is_digraph = isinstance(graph, nx.DiGraph)
    entities = []
    nodes = {}

    for i, (node_name, properties) in enumerate(graph.nodes(data=True)):
        entities.append(get_node(i, properties))
        nodes[node_name] = i

    if label:
        for i in nodes.values():
            entities.append(get_label(i, label))

    for from_node, to_node, properties in graph.edges(data=True):
        if edge_rel_key is not None:
            try:    # If `edge_rel_key` is not in this edge's properties...
                ename = properties[edge_rel_key]
            except KeyError:
                # ...attempt to default to `edge_rel_name` before complaining.
                if edge_rel_name is not None:
                    ename = edge_rel_name
                else:   # If neither are provided, raise a ValueError.
                    raise ValueError('Invalid edge label key')
        else:   # Use edge_rel_name if edge_rel_key is not provided.
            ename = edge_rel_name

        edge = get_relationship(nodes[from_node], nodes[to_node], ename,
                                properties)
        entities.append(edge)

        if not is_digraph:
            reverse_edge = get_relationship(nodes[to_node],
                                            nodes[from_node],
                                            ename, properties)
            entities.append(reverse_edge)

    return encoder.encode(entities)


def check_exception(result):
    """checks, if the preceding HTTP request was accepted by the Neo4j
    server.

    :param result: a `Response \
<http://docs.python-requests.org/en/latest/api/#requests.Response>`_
        instance.
    :rtype: an Exception or None
    """
    if result.status_code == 200:
        return

    if result.headers.get('content-type', '').lower() == JSON_CONTENT_TYPE:
        result_json = result.json()
        e = Exception(result_json['errors'])
    else:
        e = Exception("Unknown server error.")
        e.args += (result.content, )
    raise e


def get_server_urls(server_url, user, password):
    """connects to the server with a GET request and returns its answer
    (e.g. a number of URLs of REST endpoints, the server version etc.)
    as a dictionary.

    :param server_url: the URL of the Neo4j server
    :param user: A Neo4j user name.
    :param password: The password belonging to the given Neo4j user name.
    :rtype: a dictionary of parameters of the Neo4j server
    """
    result = requests.get(server_url, auth=(user, password))
    check_exception(result)

    return result.json()


def write_to_neo(server_url, graph, user, password, edge_rel_name=None,
                 label=None, encoder=None, edge_rel_key=None):
    """Write the `graph` as Geoff string. The edges between the nodes
    have relationship name `edge_rel_name`. The code
    below shows a simple example::

        from neonx import write_to_neo

        # create a graph
        import networkx as nx
        G = nx.Graph()
        G.add_nodes_from([1, 2, 3])
        G.add_edge(1, 2)
        G.add_edge(2, 3)

        # save graph to neo4j
        results = write_to_neo("http://localhost:7474/db/data/", G, \
'LINKS_TO', 'Node')

    If the properties are not json encodable, please pass a custom JSON
    encoder class. See `JSONEncoder
    <http://docs.python.org/2/library/json.html#json.JSONEncoder/>`_.

    If `label` is present, this label was be associated with all the nodes
    created. Label support were added in Neo4j 2.0. See \
    `here <http://bit.ly/1fo5324>`_.

    If the parameter `edge_rel_key` is present, neonx will look for that
    property in edge data and attempt to use its value as the relation name. If
    that property is not found, then `edge_rel_name` will be used for that
    edge. If `edge_rel_key` is not present, then `edge_rel_name` should be
    provided.:
        from neonx import write_to_neo

        # create a graph
        import networkx as nx
        G = nx.Graph()
        G.add_nodes_from([1, 2, 3])
        G.add_edge(1, 2, label='KNOWS')
        G.add_edge(2, 3)

        results = write_to_neo("http://localhost:7474/db/data/", G, \
'LINKS_TO', 'Node', edge_rel_key='label')

    :param server_url: Server URL for the Neo4j server.
    :param graph: A NetworkX Graph or a DiGraph.
    :param user: A Neo4j user name.
    :param password: The password belonging to the given Neo4j user name.
    :param optional edge_rel_name: Relationship name between the nodes.
    :param optional label: It will add this label to the node. \
See `here <http://bit.ly/1fo5324>`_.
    :param optional encoder: JSONEncoder object. Defaults to JSONEncoder.
    :param optional edge_rel_key: Key in edge attributes to use as edge label.
    :rtype: A list of Neo4j created resources.
    """

    if encoder is None:
        encoder = json.JSONEncoder()

    if edge_rel_name is None and edge_rel_key is None:
        raise ValueError(
            'Must provide either `edge_rel_name` or `edge_rel_key`')

    all_server_urls = get_server_urls(server_url, user, password)
    batch_url = all_server_urls['batch']

    data = generate_data(graph, edge_rel_name=edge_rel_name, label=label,
                         encoder=encoder, edge_rel_key=edge_rel_key)
    result = requests.post(batch_url, data=data, headers=HEADERS,
                           auth=(user, password))
    check_exception(result)
    return result.json()


LABEL_QRY = """MATCH (a:{0})-[r]->(b:{1}) RETURN ID(a), r, ID(b);"""


def get_neo_graph(server_url, label, user, password):
    """Return a graph of all nodes with a given Neo4j label and edges between
    the same nodes.

    :param server_url: Server URL for the Neo4j server.
    :param label: The label to retrieve the nodes for.
    :param user: A Neo4j user name.
    :param password: The password belonging to the given Neo4j user name.
    :rtype: A `Digraph \
<http://networkx.github.io/documentation/latest/\
reference/classes.digraph.html>`_.
    """
    all_server_urls = get_server_urls(server_url, user, password)
    batch_url = all_server_urls['batch']

    data = [{"method": "GET", "to": '/label/{0}/nodes'.format(label),
             "body": {}},
            {"method": "POST", "to": '/cypher', "body": {"query":
             LABEL_QRY.format(label, label), "params": {}}},
            ]

    result = requests.post(batch_url, data=json.dumps(data), headers=HEADERS)

    check_exception(result)

    node_data, edge_date = result.json()
    graph = nx.DiGraph()

    for n in node_data['body']:
        node_id = int(n['self'].rpartition('/')[-1])
        graph.add_node(node_id, **n['data'])

    for n in edge_date['body']['data']:
        from_node_id, relationship, to_node_id = n

        properties = relationship['data']
        properties['neo_rel_name'] = relationship['type']
        graph.add_edge(from_node_id, to_node_id, **properties)

    return graph
