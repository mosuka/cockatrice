# -*- coding: utf-8 -*-

# Copyright (c) 2019 Minoru Osuka
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 		http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import _pickle as pickle
import os
import unittest
import zipfile
from logging import ERROR, Formatter, getLogger, INFO, StreamHandler
from tempfile import TemporaryDirectory
from time import sleep

import grpc
import yaml
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.index_core import IndexCore
from cockatrice.index_grpc_server import IndexGRPCServer
from cockatrice.protobuf.index_pb2 import CloseIndexRequest, CreateIndexRequest, CreateSnapshotRequest, \
    DeleteDocumentRequest, DeleteDocumentsRequest, DeleteIndexRequest, DeleteNodeRequest, GetDocumentRequest, \
    GetIndexRequest, GetSnapshotRequest, GetStatusRequest, IsAliveRequest, IsReadyRequest, OpenIndexRequest, \
    OptimizeIndexRequest, PutDocumentRequest, PutDocumentsRequest, PutNodeRequest, SearchDocumentsRequest, \
    SnapshotExistsRequest
from cockatrice.protobuf.index_pb2_grpc import IndexStub
from tests import get_free_port


class TestIndexGRPCServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        host = '0.0.0.0'
        port = get_free_port()
        peer_addrs = []
        snapshot_file = self.temp_dir.name + '/snapshot.zip'
        grpc_port = get_free_port()

        index_dir = self.temp_dir.name + '/index'

        logger = getLogger(NAME)
        log_handler = StreamHandler()
        logger.setLevel(ERROR)
        log_handler.setLevel(INFO)
        log_format = Formatter('%(asctime)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)

        http_logger = getLogger(NAME + '_http')
        http_log_handler = StreamHandler()
        http_logger.setLevel(INFO)
        http_log_handler.setLevel(INFO)
        http_log_format = Formatter('%(message)s')
        http_log_handler.setFormatter(http_log_format)
        http_logger.addHandler(http_log_handler)

        metrics_registry = CollectorRegistry()

        conf = SyncObjConf(
            fullDumpFile=snapshot_file,
            logCompactionMinTime=300,
            dynamicMembershipChange=True
        )

        self.index_core = IndexCore(host=host, port=port, peer_addrs=peer_addrs, conf=conf, index_dir=index_dir,
                                    logger=logger, metrics_registry=metrics_registry)
        self.index_grpc_server = IndexGRPCServer(self.index_core, host=host, port=grpc_port, max_workers=10,
                                                 logger=logger, metrics_registry=metrics_registry)

        self.channel = grpc.insecure_channel('{0}:{1}'.format(host, grpc_port))

    def tearDown(self):
        self.channel.close()

        self.index_core.stop()
        self.index_grpc_server.stop()
        self.temp_dir.cleanup()

    def test_create_index(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True

        response = stub.CreateIndex(request)

        self.assertEqual(True, response.status.success)

    def test_get_index(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True

        response = stub.CreateIndex(request)

        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'

        response = stub.GetIndex(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

    def test_delete_index(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True

        response = stub.CreateIndex(request)

        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'

        response = stub.GetIndex(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # delete index
        request = DeleteIndexRequest()
        request.index_name = 'test_index'
        request.sync = True

        response = stub.DeleteIndex(request)

        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'

        response = stub.GetIndex(request)

        self.assertEqual(False, response.status.success)

    def test_open_index(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True

        response = stub.CreateIndex(request)

        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'

        response = stub.GetIndex(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # close index
        request = CloseIndexRequest()
        request.index_name = 'test_index'
        request.sync = True

        response = stub.CloseIndex(request)

        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'

        response = stub.GetIndex(request)

        self.assertEqual(False, response.status.success)

        # open index
        request = OpenIndexRequest()
        request.index_name = 'test_index'
        request.sync = True

        response = stub.OpenIndex(request)

        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'

        response = stub.GetIndex(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

    def test_close_index(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True
        response = stub.CreateIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # close index
        request = CloseIndexRequest()
        request.index_name = 'test_index'
        request.sync = True
        response = stub.CloseIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(False, response.status.success)

    def test_optimize_index(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True
        response = stub.CreateIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # optimize index
        request = OptimizeIndexRequest()
        request.index_name = 'test_index'
        request.sync = True
        response = stub.CloseIndex(request)
        self.assertEqual(True, response.status.success)

    def test_put_document(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True
        response = stub.CreateIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # read doc1.yaml
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            fields_dict = yaml.safe_load(file_obj.read())

        # put document
        request = PutDocumentRequest()
        request.index_name = 'test_index'
        request.doc_id = '1'
        request.fields = pickle.dumps(fields_dict)
        request.sync = True
        response = stub.PutDocument(request)
        self.assertEqual(True, response.status.success)

    def test_get_document(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True
        response = stub.CreateIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # read doc1.yaml
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            fields_dict = yaml.safe_load(file_obj.read())

        # put document
        request = PutDocumentRequest()
        request.index_name = 'test_index'
        request.doc_id = '1'
        request.fields = pickle.dumps(fields_dict)
        request.sync = True
        response = stub.PutDocument(request)
        self.assertEqual(1, response.count)
        self.assertEqual(True, response.status.success)

        # get document
        request = GetDocumentRequest()
        request.index_name = 'test_index'
        request.doc_id = '1'
        response = stub.GetDocument(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual('1', pickle.loads(response.fields)['id'])
        self.assertEqual('Search engine (computing)', pickle.loads(response.fields)['title'])

    def test_delete_document(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True
        response = stub.CreateIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # read doc1.yaml
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            fields_dict = yaml.safe_load(file_obj.read())

        # put document
        request = PutDocumentRequest()
        request.index_name = 'test_index'
        request.doc_id = '1'
        request.fields = pickle.dumps(fields_dict)
        request.sync = True
        response = stub.PutDocument(request)
        self.assertEqual(1, response.count)
        self.assertEqual(True, response.status.success)

        # get document
        request = GetDocumentRequest()
        request.index_name = 'test_index'
        request.doc_id = '1'
        response = stub.GetDocument(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('1', pickle.loads(response.fields)['id'])
        self.assertEqual('Search engine (computing)', pickle.loads(response.fields)['title'])

        # delete document
        request = DeleteDocumentRequest()
        request.index_name = 'test_index'
        request.doc_id = '1'
        request.sync = True
        response = stub.DeleteDocument(request)
        self.assertEqual(1, response.count)
        self.assertEqual(True, response.status.success)

        # get document
        request = GetDocumentRequest()
        request.index_name = 'test_index'
        request.doc_id = '1'
        response = stub.GetDocument(request)
        self.assertEqual(False, response.status.success)

    def test_put_documents(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True
        response = stub.CreateIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # read bulk_put.yaml
        with open(self.example_dir + '/bulk_put.yaml', 'r', encoding='utf-8') as file_obj:
            docs_dict = yaml.safe_load(file_obj.read())

        # put documents
        request = PutDocumentsRequest()
        request.index_name = 'test_index'
        request.docs = pickle.dumps(docs_dict)
        request.sync = True
        response = stub.PutDocuments(request)
        self.assertEqual(5, response.count)
        self.assertEqual(True, response.status.success)

    def test_delete_documents(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True
        response = stub.CreateIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # read bulk_put.yaml
        with open(self.example_dir + '/bulk_put.yaml', 'r', encoding='utf-8') as file_obj:
            docs_dict = yaml.safe_load(file_obj.read())

        # put documents
        request = PutDocumentsRequest()
        request.index_name = 'test_index'
        request.docs = pickle.dumps(docs_dict)
        request.sync = True
        response = stub.PutDocuments(request)
        self.assertEqual(5, response.count)
        self.assertEqual(True, response.status.success)

        # read bulk_delete.yaml
        with open(self.example_dir + '/bulk_delete.yaml', 'r', encoding='utf-8') as file_obj:
            doc_ids_list = yaml.safe_load(file_obj.read())

        # delete documents
        request = DeleteDocumentsRequest()
        request.index_name = 'test_index'
        request.doc_ids = pickle.dumps(doc_ids_list)
        request.sync = True
        response = stub.DeleteDocuments(request)
        self.assertEqual(5, response.count)
        self.assertEqual(True, response.status.success)

    def test_search_documents(self):
        stub = IndexStub(self.channel)

        # read schema.yaml
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            scheme_dict = yaml.safe_load(file_obj.read())

        # create index
        request = CreateIndexRequest()
        request.index_name = 'test_index'
        request.schema = pickle.dumps(scheme_dict)
        request.sync = True
        response = stub.CreateIndex(request)
        self.assertEqual(True, response.status.success)

        # get index
        request = GetIndexRequest()
        request.index_name = 'test_index'
        response = stub.GetIndex(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual('test_index', response.index_stats.name)

        # read bulk_put.yaml
        with open(self.example_dir + '/bulk_put.yaml', 'r', encoding='utf-8') as file_obj:
            docs_dict = yaml.safe_load(file_obj.read())

        # put documents
        request = PutDocumentsRequest()
        request.index_name = 'test_index'
        request.docs = pickle.dumps(docs_dict)
        request.sync = True
        response = stub.PutDocuments(request)
        self.assertEqual(5, response.count)
        self.assertEqual(True, response.status.success)

        # read weighting.yaml
        with open(self.example_dir + '/weighting.yaml', 'r', encoding='utf-8') as file_obj:
            weighting_dict = yaml.safe_load(file_obj.read())

        # search documents
        request = SearchDocumentsRequest()
        request.index_name = 'test_index'
        request.query = 'search'
        request.search_field = 'text'
        request.page_num = 1
        request.page_len = 10
        request.weighting = pickle.dumps(weighting_dict)
        response = stub.SearchDocuments(request)
        self.assertEqual(5, pickle.loads(response.results)['total'])
        self.assertEqual(True, response.status.success)

    def test_put_node(self):
        stub = IndexStub(self.channel)

        # get node
        request = GetStatusRequest()
        response = stub.GetStatus(request)
        self.assertEqual(0, pickle.loads(response.node_status)['partner_nodes_count'])
        self.assertEqual(True, response.status.success)

        # put node
        request = PutNodeRequest()
        request.node_name = 'localhost:{0}'.format(get_free_port())
        response = stub.PutNode(request)
        sleep(1)  # wait for node to be added
        self.assertEqual(True, response.status.success)

        # get node
        request = GetStatusRequest()
        response = stub.GetStatus(request)
        self.assertEqual(1, pickle.loads(response.node_status)['partner_nodes_count'])
        self.assertEqual(True, response.status.success)

    def test_delete_node(self):
        stub = IndexStub(self.channel)

        port = get_free_port()

        # get node
        request = GetStatusRequest()
        response = stub.GetStatus(request)
        self.assertEqual(0, pickle.loads(response.node_status)['partner_nodes_count'])
        self.assertEqual(True, response.status.success)

        # put node
        request = PutNodeRequest()
        request.node_name = 'localhost:{0}'.format(port)
        response = stub.PutNode(request)
        sleep(1)  # wait for node to be added
        self.assertEqual(True, response.status.success)

        # get node
        request = GetStatusRequest()
        response = stub.GetStatus(request)
        self.assertEqual(1, pickle.loads(response.node_status)['partner_nodes_count'])
        self.assertEqual(True, response.status.success)

        # delete node
        request = DeleteNodeRequest()
        request.node_name = 'localhost:{0}'.format(port)
        response = stub.DeleteNode(request)
        sleep(1)  # wait for node to be deleted
        self.assertEqual(True, response.status.success)

        # get node
        request = GetStatusRequest()
        response = stub.GetStatus(request)
        self.assertEqual(0, pickle.loads(response.node_status)['partner_nodes_count'])
        self.assertEqual(True, response.status.success)

    def test_snapshot_exists(self):
        stub = IndexStub(self.channel)

        # snapshot exists
        request = SnapshotExistsRequest()
        response = stub.SnapshotExists(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual(False, response.exist)

        # create snapshot
        request = CreateSnapshotRequest()
        response = stub.CreateSnapshot(request)
        sleep(1)  # wait for snapshot file to be created
        self.assertEqual(True, response.status.success)
        self.assertEqual(True, os.path.exists(self.index_core.get_snapshot_file_name()))

        # snapshot exists
        request = SnapshotExistsRequest()
        response = stub.SnapshotExists(request)
        self.assertEqual(True, response.status.success)
        self.assertEqual(True, response.exist)

    def test_create_snapshot(self):
        stub = IndexStub(self.channel)

        self.assertEqual(False, os.path.exists(self.index_core.get_snapshot_file_name()))

        # create snapshot
        request = CreateSnapshotRequest()
        response = stub.CreateSnapshot(request)
        sleep(1)  # wait for snapshot file to be created
        self.assertEqual(True, response.status.success)
        self.assertEqual(True, os.path.exists(self.index_core.get_snapshot_file_name()))

        with zipfile.ZipFile(self.index_core.get_snapshot_file_name()) as f:
            self.assertEqual(['raft.bin'], f.namelist())

    def test_get_snapshot(self):
        stub = IndexStub(self.channel)

        # create snapshot
        request = CreateSnapshotRequest()
        response = stub.CreateSnapshot(request)
        sleep(1)  # wait for snapshot file to be created
        self.assertEqual(True, response.status.success)
        self.assertEqual(True, os.path.exists(self.index_core.get_snapshot_file_name()))

        with zipfile.ZipFile(self.index_core.get_snapshot_file_name()) as f:
            self.assertEqual(['raft.bin'], f.namelist())

        # get snapshot
        request = GetSnapshotRequest()
        request.chunk_size = 1024

        response = stub.GetSnapshot(request)

        download_file_name = self.temp_dir.name + '/snapshot_downloaded.zip'

        with open(download_file_name, 'wb') as f:
            for snapshot in response:
                f.write(snapshot.chunk)

        with zipfile.ZipFile(download_file_name) as f:
            self.assertEqual(['raft.bin'], f.namelist())

    def test_is_alive(self):
        stub = IndexStub(self.channel)

        # is the node alive
        request = IsAliveRequest()

        response = stub.IsAlive(request)

        self.assertEqual(True, response.status.success)

    def test_is_ready(self):
        stub = IndexStub(self.channel)

        # is the cluster ready
        request = IsReadyRequest()

        response = stub.IsReady(request)

        self.assertEqual(True, response.status.success)

    def test_get_status(self):
        stub = IndexStub(self.channel)

        # get node
        request = GetStatusRequest()
        response = stub.GetStatus(request)
        self.assertEqual(0, pickle.loads(response.node_status)['partner_nodes_count'])
        self.assertEqual(True, response.status.success)
