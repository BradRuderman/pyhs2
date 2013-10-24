import mock
import unittest
from pyhs2.TCLIService.ttypes import TSessionHandle, TCloseSessionReq
from pyhs2.connections import Connection


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.host = "localhost"
        self.port = 10000
        self.authMechanism = "PLAIN"
        self.user = "dr_who"
        self.password = "foobar"
        self.database = "mydb"

    def create_connection(self):
        return Connection(host=self.host, port=self.port, authMechanism=self.authMechanism, user=self.user,
                          password=self.password, database=self.database)

    @mock.patch('pyhs2.connections.TCLIService')
    @mock.patch('pyhs2.connections.TSaslClientTransport')
    def test_autocloses_session_as_context_manager(self, _mock_transport, mock_tcli_service):
        mock_client = mock_tcli_service.Client.return_value

        mock_sesh_handle = mock.MagicMock()
        mock_client.OpenSession.return_value = mock_sesh_handle

        with self.create_connection() as connection:
            # We're just testing the context management behavior; don't bother doing anything
            pass

        mock_client.CloseSession.assert_called_once_with(
            TCloseSessionReq(mock_sesh_handle.sessionHandle))
