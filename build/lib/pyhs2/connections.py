import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
import sasl
from cloudera.thrift_sasl import TSaslClientTransport

from TCLIService import TCLIService
from TCLIService.ttypes import TOpenSessionReq, TGetTablesReq, TFetchResultsReq,\
  TStatusCode, TGetResultSetMetadataReq, TGetColumnsReq, TType, TTypeId, \
  TExecuteStatementReq, TGetOperationStatusReq, TFetchOrientation, TCloseOperationReq, \
  TCloseSessionReq, TGetSchemasReq, TGetLogReq, TCancelOperationReq



class Connection(object):
    session = None
    client = None

    def __init__(self, host=None, port=10000, authMechanism=None, user=None, passwd=None, db=None):
        authMechanisms = {'NOSASL', 'PLAIN', 'KERBEROS', ''}
        if authMechanism not in authMechanisms:
            raise NotImplementedError('authMechanism is either not supported or not implemented')
        socket = TSocket(host, port)
        if auth == 'NOSASL':
            transport = TBufferedTransport(socket)
        else:
            transport = TSaslClientTransport(sasl_factory, "PLAIN", socket)
        client = TCLIService.Client(TBinaryProtocol(transport))
        transport.open()

    def close(self):
        req = TCloseSessionReq(sessionHandle=session)
        client.CloseSession(req)