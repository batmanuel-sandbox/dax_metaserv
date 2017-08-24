#!/usr/bin/env python

# LSST Data Management System
# Copyright 2015 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
This is a unittest for the SchemaToMeta class.

@author  Jacek Becla, SLAC
"""

# standard library
import logging as log
import os
import tempfile
import unittest

# local
from lsst.dax.metaserv.schema_utils import parse_schema


class TestS2M(unittest.TestCase):

    def test_basics(self):
        """
        Basic test: load data for two tables.
        """
        (fd, fName) = tempfile.mkstemp()
        temp_file = os.fdopen(fd, "w")
        temp_file.write("""
CREATE TABLE t1
    -- <descr>This is t1 table.</descr>
(
    id int,
        -- <descr>the t1.id</descr>
    ra double DEFAULT 1,
        -- <descr>right asc</descr>
        -- <ucd>pos.eq.ra</ucd>
        -- <unit>deg</unit>
    decl double,
        -- <ucd>pos.eq.dec</ucd>
        -- <unit>deg</unit>
    s char DEFAULT 'x',
        -- <descr>the t1.s</descr>
    v varchar(255),
    PRIMARY KEY pk_t1_id (id),
    INDEX idx_t1_s (s)
) ENGINE=MyISAM;

CREATE TABLE t2
   -- <descr>This is
   -- t2 table.</descr>
(
    id2 int,
        -- <descr>This is a very
        -- long
        -- description of the
        -- t2.id2.</descr>
    s2 char,
        -- <descr>Description for s2.
        -- </descr>
    v2 varchar(255)
) ENGINE = InnoDB;
""")
        temp_file.close()
        parsed_tables = parse_schema(fName)
        self.assertEqual(len(parsed_tables), 2)
        self.assertEqual(len(parsed_tables["t1"]["columns"]), 5)
        self.assertEqual(parsed_tables["t1"]["columns"][0]["name"], "id")
        self.assertEqual(parsed_tables["t1"]["columns"][0]["description"], "the t1.id")
        self.assertEqual(parsed_tables["t1"]["columns"][1]["name"], "ra")
        self.assertEqual(parsed_tables["t1"]["columns"][1]["defaultValue"], "1")
        self.assertEqual(parsed_tables["t1"]["columns"][1]["description"], "right asc")
        self.assertEqual(parsed_tables["t1"]["columns"][1]["ucd"], "pos.eq.ra")
        self.assertEqual(parsed_tables["t1"]["columns"][1]["unit"], "deg")
        self.assertEqual(parsed_tables["t1"]["columns"][2]["name"], "decl")
        self.assertTrue("description" not in parsed_tables["t1"]["columns"][2])
        self.assertEqual(parsed_tables["t1"]["columns"][2]["ucd"], "pos.eq.dec")
        self.assertEqual(parsed_tables["t1"]["columns"][2]["unit"], "deg")
        self.assertEqual(parsed_tables["t1"]["columns"][3]["name"], "s")
        self.assertEqual(parsed_tables["t1"]["columns"][3]["defaultValue"], "'x'")
        self.assertEqual(parsed_tables["t1"]["columns"][3]["description"], "the t1.s")
        self.assertTrue("ucd" not in parsed_tables["t1"]["columns"][3])
        self.assertEqual(parsed_tables["t1"]["columns"][4]["name"], "v")
        self.assertTrue("description" not in parsed_tables["t1"]["columns"][4])
        self.assertTrue("ucd" not in parsed_tables["t1"]["columns"][4])
        self.assertEqual(parsed_tables["t1"]["description"], "This is t1 table.")
        self.assertEqual(parsed_tables["t1"]["engine"], "MyISAM")
        self.assertEqual(len(parsed_tables["t1"]["indexes"]), 2)
        self.assertEqual(parsed_tables["t1"]["indexes"][0]["columns"], "id")
        self.assertEqual(parsed_tables["t1"]["indexes"][0]["type"], "PRIMARY KEY")
        self.assertEqual(parsed_tables["t1"]["indexes"][1]["columns"], "s")
        self.assertEqual(parsed_tables["t2"]["description"], "This is t2 table.")
        self.assertEqual(parsed_tables["t2"]["columns"][0]["description"],
                         "This is a very long description of the t2.id2.")
        self.assertEqual(parsed_tables["t2"]["columns"][1]["description"],
                         "Description for s2.")
        self.assertEqual(parsed_tables["t2"]["engine"], "InnoDB")

    def test_comments(self):
        """
        Test commented block
        """
        (fd, fName) = tempfile.mkstemp()
        temp_file = os.fdopen(fd, "w")
        temp_file.write("""
--CREATE TABLE tDummy1
    -- <descr>This is dummy table 1.</descr>
--(
--    id int,
--    PRIMARY KEY pk_t1_id (id),
--    INDEX idx_t1_s (s)
--) ENGINE=MyISAM;

-- CREATE TABLE tDummy2
-- (
--    id int,
--    PRIMARY KEY pk_t1_id (id),
--    INDEX idx_t1_s (s)
-- ) ENGINE=MyISAM;

CREATE TABLE t3 (
    id3 int
) ENGINE =InnoDB;
""")
        temp_file.close()
        parsed_tables = parse_schema(fName)
        self.assertEqual(len(parsed_tables), 1)

    def test_float(self):
        """
        Test FLOAT(0)
        """
        (fd, fName) = tempfile.mkstemp()
        temp_file = os.fdopen(fd, "w")
        temp_file.write("""
CREATE TABLE t (
    f1 FLOAT(0),
    f2 FLOAT
);
""")
        temp_file.close()
        parsed_tables = parse_schema(fName)
        self.assertEqual(len(parsed_tables["t"]["columns"]), 2)
        self.assertEqual(parsed_tables["t"]["columns"][0]["datatype"], "float")
        self.assertEqual(parsed_tables["t"]["columns"][1]["datatype"], "float")

    def test_indices(self):
        """
        Test index lines.
        """
        (fd, fName) = tempfile.mkstemp()
        temp_file = os.fdopen(fd, "w")
        temp_file.write("""
CREATE TABLE t (
    id int,
    sId bigint,
    decl DOUBLE,
    ampName VARCHAR(64),
    xx int,
    yy int,
    PRIMARY KEY (id),
    KEY IDX_sId (sId ASC),
    INDEX IDX_d (decl DESC),
    UNIQUE UQ_AmpMap_ampName(ampName),
    UNIQUE UQ_x(xx DESC, yy)
);
""")
        temp_file.close()
        parsed_tables = parse_schema(fName)
        self.assertEqual(parsed_tables["t"]["indexes"][0]["columns"], "id")
        self.assertEqual(parsed_tables["t"]["indexes"][0]["type"], "PRIMARY KEY")
        self.assertEqual(parsed_tables["t"]["indexes"][1]["columns"], "sId")
        self.assertEqual(parsed_tables["t"]["indexes"][1]["type"], "-")
        self.assertEqual(parsed_tables["t"]["indexes"][2]["columns"], "decl")
        self.assertEqual(parsed_tables["t"]["indexes"][2]["type"], "-")
        self.assertEqual(parsed_tables["t"]["indexes"][3]["columns"], "ampName")
        self.assertEqual(parsed_tables["t"]["indexes"][3]["type"], "UNIQUE")
        self.assertEqual(parsed_tables["t"]["indexes"][4]["columns"], "xx, yy")
        self.assertEqual(parsed_tables["t"]["indexes"][3]["type"], "UNIQUE")


def main():
    log.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=log.DEBUG)

    unittest.main()

if __name__ == "__main__":
    main()
