import json
import unittest
from flask import Flask
from lsst.dax.metaserv import api_v0
from mock import MagicMock

import MySQLdb


class MockResults(list):
    def __init__(self, seq=(), description=None):
        super(MockResults, self).__init__(seq)
        self.cursor = MagicMock()
        self.cursor.description = description
        self.cursor.description_flags = [0 for _ in description]
        # If first is called, just return the first row
        self.first = lambda: self


def mysql_desc(col):
    name = str(col)
    typ = MySQLdb.constants.FIELD_TYPE.DOUBLE
    if isinstance(col, str):
        typ = MySQLdb.constants.FIELD_TYPE.STRING
    return name, typ, None, None, None, 0, None, None


class TestMySqlQuery(unittest.TestCase):

    urls = {
        "/meta/v0/db":
            "SELECT DISTINCT lsstLevel FROM Repo WHERE repoType = 'db'",
        "/meta/v0/db/L2":
            "SELECT dbName FROM Repo JOIN DbRepo on (repoId=dbRepoId) WHERE lsstLevel = :lsstLevel",
        "/meta/v0/db/L2/DC_W13_Stripe82/tables":
            "SELECT table_name FROM information_schema.tables WHERE table_schema=:dbName",
        "/meta/v0/db/L2/DC_W13_Stripe82/tables/Science_Ccd_Exposure/schema":
            "SHOW CREATE TABLE DC_W13_Stripe82.Science_Ccd_Exposure"
    }

    queries = {
        # Scalar result
        "SELECT DISTINCT lsstLevel FROM Repo WHERE repoType = 'db'": ["L2"],
        # Vector result
        "SELECT dbName FROM Repo JOIN DbRepo on (repoId=dbRepoId) WHERE lsstLevel = :lsstLevel":
            [["metaServ_baselineSchema"]],
        # Vector result (result is truncated for test brevity)
        "SELECT table_name FROM information_schema.tables WHERE table_schema=:dbName":
            [["AvgForcedPhot"], ["AvgForcedPhotYearly"]],
        # Scalar result (result is truncated for test brevity)
        "SHOW CREATE TABLE DC_W13_Stripe82.Science_Ccd_Exposure":
            ["Science_Ccd_Exposure",
             "CREATE TABLE `Science_Ccd_Exposure` (\n  `scienceCcdExposureId` bigint(20) NOT NULL,\n"
             ") ENGINE=MyISAM DEFAULT CHARSET=latin1"]
    }

    def setUp(self):
        self.app = Flask(__name__)
        self.client = self.app.test_client()
        self.mock_engine = MagicMock()
        self.app.config['default_engine'] = self.mock_engine
        self.app.register_blueprint(api_v0.metaREST, url_prefix='/meta/v0')

        def side_effect(query, **kwargs):
            # kwargs are passed, but it's not necessary we use them for anything, so we ignore
            arg = str(query)  # This is actually a sqlalchemy.text object, convert to string
            return MockResults(self.queries[arg], [mysql_desc(c) for c in self.queries[arg][0]])

        self.mock_engine.execute.side_effect = side_effect

    def test_basic_queries_json(self):

        for url, query in self.urls.items():
            resp = self.client.get(url)
            expected_results = self.queries[query]
            json_resp = json.loads(resp.data)
            if "results" in json_resp:
                self.assertEqual(json_resp["results"], expected_results)
            if "result" in json_resp:
                self.assertEqual(json_resp["result"], expected_results)

    def test_basic_queries_html(self):
        for url, query in self.urls.items():
            resp = self.client.get(url, headers={"accept": "text/html"})
            expected_results = self.queries[query]
            # Note: Trim spaces because jinja2 rendering adds spaces in the case of schema rendering
            if isinstance(expected_results[0], list):
                expected_row = "<td>" + "</td><td>".join([str(i) for i in expected_results[0]]) + "</td>"
                self.assertIn(expected_row.replace(" ", ""), resp.data.replace(" ", ""))
            else:
                expected_row = "<td>" + "</td><td>".join([str(i) for i in expected_results]) + "</td>"
                self.assertIn(expected_row.replace(" ", ""), resp.data.replace(" ", ""))


if __name__ == '__main__':
    unittest.main()
