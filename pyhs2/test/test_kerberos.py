import unittest
from pyhs2.connections import Connection

@unittest.skip("These are not proper unit tests. A secure hive server is required to actually execute them.")
class KerberosTest(unittest.TestCase):

    def create_conn(self):
        return Connection(host="secure.hiveserver2.fqdn", user="kerberos_principal@realm", password="password", authMechanism="KERBEROS")

    def test_meta(self):
        with(self.create_conn()) as conn:
            with(conn.cursor()) as cur:
                databases = cur.getDatabases()
                print(databases)
                self.assertIsNotNone(databases)
                self.assertGreater(len(databases), 0)

    def test_querying(self):
        with(self.create_conn()) as conn:
            with(conn.cursor()) as cur:
                cur.execute("SELECT count(*) FROM test_table")
                record = cur.fetch()
                print(record)
                self.assertIsNotNone(record)
                self.assertGreater(len(record), 0)
