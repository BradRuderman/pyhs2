import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
import sasl
from cloudera.thrift_sasl import TSaslClientTransport

from TCLIService import TCLIService

from cursor import Cursor
from TCLIService.ttypes import TCloseSessionReq,TOpenSessionReq

class Connection(object):
    client = None
    session = None

    def __init__(self, host=None, port=10000, authMechanism=None, user=None, password=None, database=None):
        authMechanisms = set(['NOSASL', 'PLAIN', 'KERBEROS', 'LDAP'])
        if authMechanism not in authMechanisms or authMechanism == 'KERBEROS':
            raise NotImplementedError('authMechanism is either not supported or not implemented')
        #Must set a password for thrift, even if it doesn't need one
        #Open issue with python-sasl
        if authMechanism = 'PLAIN' and (password is None or len(password) == 0):
            password = 'password'
        socket = TSocket(host, port)
        if authMechanism == 'NOSASL':
            transport = TBufferedTransport(socket)
        else:
            saslc = sasl.Client()
            saslc.setAttr("username", user)
            saslc.setAttr("password", password)
            saslc.init()
            transport = TSaslClientTransport(saslc, "PLAIN", socket)
        self.client = TCLIService.Client(TBinaryProtocol(transport))
        transport.open()
        res = self.client.OpenSession(TOpenSessionReq())
        self.session = res.sessionHandle
        if database is not None:
            with self.cursor() as cur:
                query = "USE {0}".format(database)
                cur.execute(query) 

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def cursor(self):
        return Cursor(self.client, self.session)

    def close(self):
        req = TCloseSessionReq(sessionHandle=self.session)
        self.client.CloseSession(req)