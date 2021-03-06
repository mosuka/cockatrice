# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from cockatrice.protobuf import index_pb2 as cockatrice_dot_protobuf_dot_index__pb2


class IndexStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.CreateIndex = channel.unary_unary(
        '/protobuf.Index/CreateIndex',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.CreateIndexRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.CreateIndexResponse.FromString,
        )
    self.DeleteIndex = channel.unary_unary(
        '/protobuf.Index/DeleteIndex',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteIndexRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteIndexResponse.FromString,
        )
    self.OpenIndex = channel.unary_unary(
        '/protobuf.Index/OpenIndex',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.OpenIndexRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.OpenIndexResponse.FromString,
        )
    self.CloseIndex = channel.unary_unary(
        '/protobuf.Index/CloseIndex',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.CloseIndexRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.CloseIndexResponse.FromString,
        )
    self.GetIndex = channel.unary_unary(
        '/protobuf.Index/GetIndex',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.GetIndexRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.GetIndexResponse.FromString,
        )
    self.CommitIndex = channel.unary_unary(
        '/protobuf.Index/CommitIndex',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.CommitIndexRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.CommitIndexResponse.FromString,
        )
    self.RollbackIndex = channel.unary_unary(
        '/protobuf.Index/RollbackIndex',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.RollbackIndexRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.RollbackIndexResponse.FromString,
        )
    self.OptimizeIndex = channel.unary_unary(
        '/protobuf.Index/OptimizeIndex',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.OptimizeIndexRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.OptimizeIndexResponse.FromString,
        )
    self.PutDocument = channel.unary_unary(
        '/protobuf.Index/PutDocument',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.PutDocumentRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.PutDocumentResponse.FromString,
        )
    self.GetDocument = channel.unary_unary(
        '/protobuf.Index/GetDocument',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.GetDocumentRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.GetDocumentResponse.FromString,
        )
    self.DeleteDocument = channel.unary_unary(
        '/protobuf.Index/DeleteDocument',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteDocumentRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteDocumentResponse.FromString,
        )
    self.PutDocuments = channel.unary_unary(
        '/protobuf.Index/PutDocuments',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.PutDocumentsRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.PutDocumentsResponse.FromString,
        )
    self.DeleteDocuments = channel.unary_unary(
        '/protobuf.Index/DeleteDocuments',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteDocumentsRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteDocumentsResponse.FromString,
        )
    self.SearchDocuments = channel.unary_unary(
        '/protobuf.Index/SearchDocuments',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.SearchDocumentsRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.SearchDocumentsResponse.FromString,
        )
    self.PutNode = channel.unary_unary(
        '/protobuf.Index/PutNode',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.PutNodeRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.PutNodeResponse.FromString,
        )
    self.DeleteNode = channel.unary_unary(
        '/protobuf.Index/DeleteNode',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteNodeRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteNodeResponse.FromString,
        )
    self.IsSnapshotExist = channel.unary_unary(
        '/protobuf.Index/IsSnapshotExist',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.IsSnapshotExistRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.IsSnapshotExistResponse.FromString,
        )
    self.CreateSnapshot = channel.unary_unary(
        '/protobuf.Index/CreateSnapshot',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.CreateSnapshotRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.CreateSnapshotResponse.FromString,
        )
    self.GetSnapshot = channel.unary_stream(
        '/protobuf.Index/GetSnapshot',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.GetSnapshotRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.GetSnapshotResponse.FromString,
        )
    self.IsHealthy = channel.unary_unary(
        '/protobuf.Index/IsHealthy',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.IsHealthyRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.IsHealthyResponse.FromString,
        )
    self.IsAlive = channel.unary_unary(
        '/protobuf.Index/IsAlive',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.IsAliveRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.IsAliveResponse.FromString,
        )
    self.IsReady = channel.unary_unary(
        '/protobuf.Index/IsReady',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.IsReadyRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.IsReadyResponse.FromString,
        )
    self.GetStatus = channel.unary_unary(
        '/protobuf.Index/GetStatus',
        request_serializer=cockatrice_dot_protobuf_dot_index__pb2.GetStatusRequest.SerializeToString,
        response_deserializer=cockatrice_dot_protobuf_dot_index__pb2.GetStatusResponse.FromString,
        )


class IndexServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def CreateIndex(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteIndex(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def OpenIndex(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def CloseIndex(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetIndex(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def CommitIndex(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def RollbackIndex(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def OptimizeIndex(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def PutDocument(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetDocument(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteDocument(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def PutDocuments(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteDocuments(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SearchDocuments(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def PutNode(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteNode(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def IsSnapshotExist(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def CreateSnapshot(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetSnapshot(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def IsHealthy(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def IsAlive(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def IsReady(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetStatus(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_IndexServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'CreateIndex': grpc.unary_unary_rpc_method_handler(
          servicer.CreateIndex,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.CreateIndexRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.CreateIndexResponse.SerializeToString,
      ),
      'DeleteIndex': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteIndex,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteIndexRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteIndexResponse.SerializeToString,
      ),
      'OpenIndex': grpc.unary_unary_rpc_method_handler(
          servicer.OpenIndex,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.OpenIndexRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.OpenIndexResponse.SerializeToString,
      ),
      'CloseIndex': grpc.unary_unary_rpc_method_handler(
          servicer.CloseIndex,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.CloseIndexRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.CloseIndexResponse.SerializeToString,
      ),
      'GetIndex': grpc.unary_unary_rpc_method_handler(
          servicer.GetIndex,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.GetIndexRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.GetIndexResponse.SerializeToString,
      ),
      'CommitIndex': grpc.unary_unary_rpc_method_handler(
          servicer.CommitIndex,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.CommitIndexRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.CommitIndexResponse.SerializeToString,
      ),
      'RollbackIndex': grpc.unary_unary_rpc_method_handler(
          servicer.RollbackIndex,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.RollbackIndexRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.RollbackIndexResponse.SerializeToString,
      ),
      'OptimizeIndex': grpc.unary_unary_rpc_method_handler(
          servicer.OptimizeIndex,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.OptimizeIndexRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.OptimizeIndexResponse.SerializeToString,
      ),
      'PutDocument': grpc.unary_unary_rpc_method_handler(
          servicer.PutDocument,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.PutDocumentRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.PutDocumentResponse.SerializeToString,
      ),
      'GetDocument': grpc.unary_unary_rpc_method_handler(
          servicer.GetDocument,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.GetDocumentRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.GetDocumentResponse.SerializeToString,
      ),
      'DeleteDocument': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteDocument,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteDocumentRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteDocumentResponse.SerializeToString,
      ),
      'PutDocuments': grpc.unary_unary_rpc_method_handler(
          servicer.PutDocuments,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.PutDocumentsRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.PutDocumentsResponse.SerializeToString,
      ),
      'DeleteDocuments': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteDocuments,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteDocumentsRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteDocumentsResponse.SerializeToString,
      ),
      'SearchDocuments': grpc.unary_unary_rpc_method_handler(
          servicer.SearchDocuments,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.SearchDocumentsRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.SearchDocumentsResponse.SerializeToString,
      ),
      'PutNode': grpc.unary_unary_rpc_method_handler(
          servicer.PutNode,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.PutNodeRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.PutNodeResponse.SerializeToString,
      ),
      'DeleteNode': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteNode,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteNodeRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.DeleteNodeResponse.SerializeToString,
      ),
      'IsSnapshotExist': grpc.unary_unary_rpc_method_handler(
          servicer.IsSnapshotExist,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.IsSnapshotExistRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.IsSnapshotExistResponse.SerializeToString,
      ),
      'CreateSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.CreateSnapshot,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.CreateSnapshotRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.CreateSnapshotResponse.SerializeToString,
      ),
      'GetSnapshot': grpc.unary_stream_rpc_method_handler(
          servicer.GetSnapshot,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.GetSnapshotRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.GetSnapshotResponse.SerializeToString,
      ),
      'IsHealthy': grpc.unary_unary_rpc_method_handler(
          servicer.IsHealthy,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.IsHealthyRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.IsHealthyResponse.SerializeToString,
      ),
      'IsAlive': grpc.unary_unary_rpc_method_handler(
          servicer.IsAlive,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.IsAliveRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.IsAliveResponse.SerializeToString,
      ),
      'IsReady': grpc.unary_unary_rpc_method_handler(
          servicer.IsReady,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.IsReadyRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.IsReadyResponse.SerializeToString,
      ),
      'GetStatus': grpc.unary_unary_rpc_method_handler(
          servicer.GetStatus,
          request_deserializer=cockatrice_dot_protobuf_dot_index__pb2.GetStatusRequest.FromString,
          response_serializer=cockatrice_dot_protobuf_dot_index__pb2.GetStatusResponse.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'protobuf.Index', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
