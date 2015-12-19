
# -*- coding: utf-8 -*-

"""
test_neo
----------------------------------

Tests for `neo` module.

NOTE: Current versions of Neo4J will not work without authentification.
Therefore, you will have to set the `NEO4J_USER` and `NEO4J_PASS` variables
in your environment in order to run these tests, e.g.:

NEO4J_USER=neo4j NEO4J_PASS=secret python setup.py test
"""

import os
import json
import unittest

from neonx.neo import generate_data, write_to_neo, get_neo_graph

import httpretty
import networkx as nx


BATCH_URL = '{"batch":"http://localhost:7474/db/data/batch"}'
NEO4J_USER = os.environ['NEO4J_USER']
NEO4J_PASS = os.environ['NEO4J_PASS']


class TestGenerateNeoData(unittest.TestCase):

    @httpretty.activate
    def test_get_geoff_digraph(self):
        truth = [{'body': {}, 'id': 0, 'method': 'POST', 'to': '/node'},
                 {'body': {}, 'id': 1, 'method': 'POST', 'to': '/node'},
                 {'body': {'debug': 'test'}, 'id': 2, 'method': 'POST',
                  'to': '/node'},
                 {'body': "ITEM", 'method': 'POST', 'to': '{0}/labels'},
                 {'body': "ITEM", 'method': 'POST', 'to': '{1}/labels'},
                 {'body': "ITEM", 'method': 'POST', 'to': '{2}/labels'},
                 {'body': {'data': {'debug': False}, 'to': '{1}',
                  'type': 'LINK_TO'},
                  'method': 'POST', 'to': '{0}/relationships'},
                 {'body': {'data': {}, 'to': '{2}', 'type': 'LINK_TO'},
                  'method': 'POST',
                  'to': '{0}/relationships'}]

        graph = nx.balanced_tree(2, 1, create_using=nx.DiGraph())
        graph.node[2]['debug'] = 'test'
        graph[0][1]['debug'] = False
        result = generate_data(graph, "LINK_TO", "ITEM", json.JSONEncoder())
        self.assertEqual(json.loads(result), truth)

        httpretty.register_uri(httpretty.GET,
                               "http://localhost:7474/db/data/",
                               body=BATCH_URL)

        httpretty.register_uri(httpretty.POST,
                               "http://localhost:7474/db/data/batch",
                               body='["Dummy"]')

        result = write_to_neo("http://localhost:7474/db/data/", graph,
                              edge_rel_name="LINKS_TO", label="ITEM",
                              user=NEO4J_USER, password=NEO4J_PASS)

        self.assertEqual(result, ["Dummy"])

    @httpretty.activate
    def test_get_geoff_graph(self):
        truth = [{'body': {}, 'id': 0, 'method': 'POST', 'to': '/node'},
                 {'body': {}, 'id': 1, 'method': 'POST', 'to': '/node'},
                 {'body': {'debug': 'test'}, 'id': 2, 'method': 'POST',
                  'to': '/node'},
                 {'body': "ITEM", 'method': 'POST', 'to': '{0}/labels'},
                 {'body': "ITEM", 'method': 'POST', 'to': '{1}/labels'},
                 {'body': "ITEM", 'method': 'POST', 'to': '{2}/labels'},
                 {'body': {'data': {'debug': False}, 'to': '{1}',
                  'type': 'LINK_TO'},
                  'method': 'POST', 'to': '{0}/relationships'},
                 {'body': {'data': {'debug': False}, 'to': '{0}',
                  'type': 'LINK_TO'},
                  'method': 'POST', 'to': '{1}/relationships'},
                 {'body': {'data': {}, 'to': '{2}', 'type': 'LINK_TO'},
                  'method': 'POST', 'to': '{0}/relationships'},
                 {'body': {'data': {}, 'to': '{0}', 'type': 'LINK_TO'},
                  'method': 'POST', 'to': '{2}/relationships'}]

        graph = nx.balanced_tree(2, 1)
        graph.node[2]['debug'] = 'test'
        graph[0][1]['debug'] = False
        result = generate_data(graph, "LINK_TO", "ITEM", json.JSONEncoder())
        self.assertEqual(json.loads(result), truth)

        httpretty.register_uri(httpretty.GET,
                               "http://localhost:7474/db/data/",
                               body=BATCH_URL)

        httpretty.register_uri(httpretty.POST,
                               "http://localhost:7474/db/data/batch",
                               body='["Dummy"]')
        result = write_to_neo("http://localhost:7474/db/data/", graph,
                              edge_rel_name="LINKS_TO", label="ITEM",
                              user=NEO4J_USER, password=NEO4J_PASS)
        self.assertEqual(result, ["Dummy"])

    @httpretty.activate
    def test_failure_500(self):
        graph = nx.balanced_tree(2, 1)

        httpretty.register_uri(httpretty.GET,
                               "http://localhost:7474/db/data/",
                               body=BATCH_URL)

        httpretty.register_uri(httpretty.POST,
                               "http://localhost:7474/db/data/batch",
                               body='Server Error', status=500,
                               content_type='text/html')

        f = lambda: write_to_neo("http://localhost:7474/db/data/", graph,
                                 edge_rel_name="LINKS_TO", label="ITEM",
                                 user=NEO4J_USER, password=NEO4J_PASS)
        self.assertRaises(Exception, f)

    @httpretty.activate
    def test_failure_json(self):
        graph = nx.balanced_tree(2, 1)

        httpretty.register_uri(httpretty.GET,
                               "http://localhost:7474/db/data/",
                               body=BATCH_URL)

        httpretty.register_uri(httpretty.POST,
                               "http://localhost:7474/db/data/batch",
                               body='{"exception": "Error", "stacktrace": 1}',
                               status=500,
                               content_type='application/json; charset=UTF-8')

        f = lambda: write_to_neo("http://localhost:7474/db/data/", graph,
                                 edge_rel_name="LINK_TO",
                                 user=NEO4J_USER, password=NEO4J_PASS)
        self.assertRaises(Exception, f)


class TestGetGraph(unittest.TestCase):

    @httpretty.activate
    def test_get_digraph(self):
        node_data = [{"data": {"name": "b"},
                     "self": "http://localhost:7474/db/data/node/1"},
                     {"data": {"name": "a"},
                      "self": "http://localhost:7474/db/data/node/2"}]
        edge_data = [[1, {"data": {"date": "2011-01-01"},
                     "type": "LINKS_TO"}, 2]]
        truth = [{"body": node_data}, {"body": {"data": edge_data}}]

        httpretty.register_uri(httpretty.GET,
                               "http://localhost:7474/db/data/",
                               body=BATCH_URL)

        httpretty.register_uri(httpretty.POST,
                               "http://localhost:7474/db/data/batch",
                               body=json.dumps(truth),
                               content_type='application/json; charset=UTF-8')

        graph = get_neo_graph("http://localhost:7474/db/data/", "Node",
                              user=NEO4J_USER, password=NEO4J_PASS)

        self.assertTrue(isinstance(graph, nx.DiGraph))
        self.assertEqual(graph.number_of_nodes(), 2)
        self.assertEqual(graph.number_of_edges(), 1)

        self.assertTrue(graph.has_node(1))
        self.assertEqual(graph.node[1]["name"], "b")

        self.assertTrue(graph.has_node(2))
        self.assertEqual(graph.node[2]["name"], "a")

        self.assertTrue(graph.has_edge(1, 2))
        self.assertEqual(graph.edge[1][2]['neo_rel_name'], "LINKS_TO")
        self.assertEqual(graph.edge[1][2]['date'], "2011-01-01")


class TestEdgeLabels(unittest.TestCase):
    def setUp(self):
        self.encoder = json.JSONEncoder()
        self.graph = nx.Graph()
        self.graph.add_edge(0, 1, label='KNOWS')
        self.graph.add_edge(1, 2, label='LIKES')
        self.graph.add_edge(2, 0, joe='BLOGGS')

    def test_no_rel_name_or_key(self):
        """
        `write_to_neo` should raise a ValueError if both `edge_rel_name`
        and `edge_rel_key` are missing.

        `generate_data` should raise a ValueError if both `edge_rel_name`
        and `edge_rel_key` are missing.
        """
        
        f = lambda: write_to_neo("http://localhost:7474/db/data/", self.graph,
                                 user=NEO4J_USER, password=NEO4J_PASS)
        self.assertRaises(ValueError, f)

        f = lambda: generate_data(self.graph, encoder=self.encoder)
        self.assertRaises(ValueError, f)

    def test_edge_rel_defaults(self):
        """
        If `edge_rel_key` is not found in an edge's properties, then
        `generate_data` should attempt to default to the value of
        `edge_rel_name`. If `edge_rel_name` is not provided, should raise
        a ValueError.
        """

        # `self.graph` contains an edge without the `label` property.
        try:
            generate_data(self.graph, edge_rel_name='LINKED_TO',
                          edge_rel_key='label', encoder=self.encoder)
        except ValueError:
            self.fail()

        # No `edge_rel_name` to which to default.
        f = lambda: generate_data(self.graph, edge_rel_key='label',
                                  encoder=self.encoder)
        self.assertRaises(ValueError, f)

    def test_edge_rel_key(self):
        """
        If `edge_rel_key` is provided, then it should be used to generate
        relation names.
        """
        edge_rel_name = 'LINKED_TO'
        edge_rel_key = 'label'
        data = json.loads(generate_data(self.graph,
                                        edge_rel_name=edge_rel_name,
                                        edge_rel_key=edge_rel_key,
                                        encoder=self.encoder))

        for datum in data:
            if datum['to'].endswith('relationships'):
                src = int(datum['to'].split('/')[0][1:-1])
                tgt = int(datum['body']['to'][1:-1])
                label = datum['body']['type']
                props = self.graph.edge[src][tgt]

                # If `edge_rel_key` was in the edge property, it should have
                #  been used as the relation name.
                if edge_rel_key in props:
                    self.assertEqual(props[edge_rel_key], label)

                # Otherwise, `edge_rel_name` should have been used.
                else:
                    self.assertEqual(edge_rel_name, label)

        #~ import pudb; pudb.set_trace()
        # Writing the resulting requests should not raise any exceptions.
        try:
            write_to_neo(
                "http://localhost:7474/db/data/", self.graph,
                edge_rel_name=edge_rel_name, edge_rel_key=edge_rel_key,
                user=NEO4J_USER, password=NEO4J_PASS)
        except Exception as e:
            self.fail('Could not write graph to neo4j. {}'.format(e))


if __name__ == '__main__':
    unittest.main()
