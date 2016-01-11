from thrift.transport.TTransport import TBufferedTransport
from TCLIService.ttypes import TOpenSessionReq, TCloseSessionReq
from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from TCLIService import TCLIService
from cursor import Cursor
from twitter.thrift_sasl import TSaslClientTransport


class Connection(object):
    def __init__(self, host=None, port=10000, authMechanism="PLAIN", username=None, password=None, database=None,
                 configuration=None, timeout=None):
        authMechanisms = {"PLAIN", "NOSASL"}
        if authMechanism not in authMechanisms:
            raise NotImplementedError("authMechanism '{}' is either not supported or not implemented".format(authMechanism))

        socket = TSocket.TSocket(host, port)
        socket.setTimeout(timeout)

        if authMechanism == "NOSASL":
            transport = TBufferedTransport(socket)
        else:  # authMechanism == "PLAIN":
            password = "password" if (password is None or len(password) == 0) else password
            transport = TSaslClientTransport(socket, host=host, service=None, mechanism=authMechanism,
                                             username=username, password=password)
        self.client = TCLIService.Client(TBinaryProtocol(transport))
        transport.open()
        res = self.client.OpenSession(TOpenSessionReq(username=username, password=password, configuration=configuration))
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
