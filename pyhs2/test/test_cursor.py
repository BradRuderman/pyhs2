import mock
import unittest
from pyhs2.TCLIService.ttypes import TSessionHandle, TCloseOperationReq
from pyhs2.cursor import Cursor


class TestCursor(unittest.TestCase):

    def setUp(self):
        self.mock_client = mock.MagicMock()
        self.session_handle = TSessionHandle(sessionId=2)
        self.to_execute = "SHOW TABLES"

    def create_cursor(self):
        return Cursor(self.mock_client, self.session_handle)

    def test_autocloses_operation_as_context_manager(self):
        mock_op_handle = mock.MagicMock()
        self.mock_client.ExecuteStatement.return_value = mock_op_handle
        with self.create_cursor() as cursor:
            cursor.execute(self.to_execute)

        self.mock_client.CloseOperation.assert_called_once_with(
            TCloseOperationReq(mock_op_handle.operationHandle))
