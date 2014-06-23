from thrift.TTornado import TTornadoStreamTransport

from thrift.protocol.TBinaryProtocol import TBinaryProtocol, TBinaryProtocolFactory
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
import sasl
from tornado import gen
from cloudera.thrift_sasl import TSaslClientTransport

from TCLIService import TCLIService
from TCLIServiceTornado import TCLIService as TCLIServiceTornado

from cursor import Cursor
from TCLIService.ttypes import TCloseSessionReq, TOpenSessionReq
from TCLIServiceTornado.ttypes import TCloseSessionReq as TCloseSessionReqTornado, \
    TOpenSessionReq as TOpenSessionReqTornado
from pyhs2.cloudera.thrift_sasl_tornado import TSaslClientTransportTornado
from pyhs2.cursor_tornado import TornadoCursor


class BaseConnection(object):
    DEFAULT_KRB_SERVICE = 'hive'
    AUTH_MECHANISMS = {'NOSASL', 'PLAIN', 'KERBEROS', 'LDAP'}
    client = None
    session = None

    def __init__(self, authMechanism):
        if authMechanism not in self.AUTH_MECHANISMS:
            raise NotImplementedError('authMechanism is either not supported or not implemented')

    def _get_krb_settings(self, default_host, config):
        host = default_host
        service = self.DEFAULT_KRB_SERVICE

        if config is not None:
            if 'krb_host' in config:
                host = config['krb_host']

            if 'krb_service' in config:
                service = config['krb_service']

        return host, service

    @staticmethod
    def _check_password(authMechanism, password):
        if authMechanism == 'PLAIN' and (password is None or len(password) == 0):
            password = 'password'
        return password

    def _get_sasl_client(self, host, authMechanism, user, password, configuration):
        sasl_mech = 'PLAIN'
        saslc = sasl.Client()
        saslc.setAttr("username", user)
        saslc.setAttr("password", password)
        if authMechanism == 'KERBEROS':
            krb_host,krb_service = self._get_krb_settings(host, configuration)
            sasl_mech = 'GSSAPI'
            saslc.setAttr("host", krb_host)
            saslc.setAttr("service", krb_service)
        saslc.init()
        return saslc, sasl_mech

class Connection(BaseConnection):
    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def __init__(self, host=None, port=10000, authMechanism=None, user=None, password=None, database=None, configuration=None):
        super(Connection, self).__init__(authMechanism)
        #Must set a password for thrift, even if it doesn't need one
        #Open issue with python-sasl
        password = self._check_password(authMechanism, password)
        socket = TSocket(host, port)
        if authMechanism == 'NOSASL':
            transport = TBufferedTransport(socket)
        else:
            saslc, sasl_mech = self._get_sasl_client(host, authMechanism, user, password, configuration)
            transport = TSaslClientTransport(saslc, sasl_mech, socket)

        self.client = TCLIService.Client(TBinaryProtocol(transport))
        transport.open()
        res = self.client.OpenSession(TOpenSessionReq(username=user, password=password, configuration=configuration))
        self.session = res.sessionHandle
        if database is not None:
            with self.cursor() as cur:
                query = "USE {0}".format(database)
                cur.execute(query)

    def cursor(self):
        return Cursor(self.client, self.session)

    def close(self):
        req = TCloseSessionReq(sessionHandle=self.session)
        self.client.CloseSession(req)

class TornadoConnection(BaseConnection):
    def __init__(self, host=None, port=10000, authMechanism=None, user=None, password=None, configuration=None):
        super(TornadoConnection, self).__init__(authMechanism)
        #Must set a password for thrift, even if it doesn't need one
        #Open issue with python-sasl
        password = self._check_password(authMechanism, password)
        if authMechanism == "NOSASL":
            self.transport = TTornadoStreamTransport(host, port)
        else:
            saslc, sasl_mech = self._get_sasl_client(host, authMechanism, user, password, configuration)
            self.transport = TSaslClientTransportTornado(saslc, sasl_mech, host, port)
        pfactory = TBinaryProtocolFactory()
        self.client = TCLIServiceTornado.Client(self.transport, pfactory)

    @gen.engine
    def connect(self, database, configuration, callback):
        yield gen.Task(self.transport.open)
        req = TOpenSessionReqTornado(configuration=configuration)
        res = yield gen.Task(self.client.OpenSession, req)
        self.session = res.sessionHandle
        if database is not None:
            query = "USE {0}".format(database)
            yield gen.Task(self.cursor().execute, query)
        callback()

    @gen.engine
    def close(self, callback):
        req = TCloseSessionReqTornado(sessionHandle=self.session)
        yield gen.Task(self.client.CloseSession, req)
        self.transport.close()
        callback()

    def cursor(self):
        return TornadoCursor(self.client, self.session)
