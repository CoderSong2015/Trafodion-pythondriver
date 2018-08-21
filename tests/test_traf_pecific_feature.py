import unittest
from pdbc.trafodion import connector

class TestTrafFeature(unittest.TestCase):
    @unittest.skip('need manually add to config.py')
    def test_multi_tenant(self):
        pass

    @unittest.skip("unsupported by python driver")
    def test_multi_IP(self):
        pass

    @unittest.skip("unsupported by python driver")
    def test_multi_DCS(self):
        pass

    @unittest.skip("unsupported by python driver")
    def test_kerberos_authentication(self):
        pass

    @unittest.skip("unsupported by python driver")
    def test_multi_query(self):
        pass