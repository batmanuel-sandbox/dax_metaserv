#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2008-2015 LSST Corporation.
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


from builtins import str
import os
import pprint
import re
import sys

"""
SchemaToMeta class parses mysql schema file that can optionally contain
extra tokens in comments. Extracts information for each table:
* name
* engine
* per column:
  - type
  - notnull
  - defaultValue
  - <descr>...</descr>
  - <unit>...</unit>
  - <ucd>...</ucd>
and saves it in an array.

Note that the SchemaToMeta expects the input file to be structured in certain
way, e.g., it will not parse any sql-compliant structure. A comprehensive
set of examples can be found in the tests/testSchemaToMeta.py.
In addition, the cat/sql/baselineSchema.py is a good "template".

This code was originally written for schema browser
(in cat/bin/schema_to_metadata.py).
"""

_tableStart = re.compile(r'CREATE TABLE (\w+)')
_tableEnd = re.compile(r"\)")
_engineLine = re.compile(r'\)\s*(ENGINE|TYPE)\s*=[\s]*(\w+)\s*;')
_columnLine = re.compile(r'\s*(\w+)\s+\w+')
_idxCols = re.compile(r'\((.+?)\)')
_unitLine = re.compile(r'<unit>(.+)</unit>')
_ucdLine = re.compile(r'<ucd>(.+)</ucd>')
_descrLine = re.compile(r'<descr>(.+)</descr>')
_descrStart = re.compile(r'<descr>(.+)')
_descrMiddle = re.compile(r'--(.+)')
_descrEnd = re.compile(r'--(.*)</descr>')
_commentLine = re.compile(r'\s*--')
_defaultLine = re.compile(r'\s+DEFAULT\s+(.+?)[\s,]')
_size_parameter = re.compile(r'\((.+)\)')

MYSQL_TYPE_MAP = {
    'VARCHAR': "text",
    'TIMESTAMP': "timestamp",
    'BINARY': "binary",
    'TINYINT': "short",
    'BIGINT': "long",
    'BIT': "boolean",
    'FLOAT': "float",
    'INT': "int",
    'INTEGER': "int",
    'DOUBLE': "double",
    'CHAR': "text"
    }


def parse_schema(schema_file_path):
    """Do actual parsing. Returns the retrieved structure as a table. The
    structure of the produced table:
{ <tableName1>: {
    'columns': [ { 'defaultValue': <value>,
                   'description': <column description>,
                   'name': <value>,
                   'nullable': <value>,
                   'ord_pos': <value>,
                   'type': <type> },
                 # repeated for every column
               ]
    'description': <table description>,
    'engine': <engine>,
    'indexes': [ { 'columns': <column name>,
                   'type': <type>},
                 # repeated for every index
               ]
  }
  # repeated for every table
}
"""

    if not os.path.isfile(schema_file_path):
        sys.stderr.write("Schema File '%s' does not exist\n" % schema_file_path)
        sys.exit(1)

    schema_file = open(schema_file_path, mode='r')

    table = None
    column = None
    column_description = None
    schema = {}

    for line in schema_file:
        m = _tableStart.search(line)
        if m is not None and not _isCommentLine(line):
            table_name = m.group(1)
            table = schema.setdefault(table_name, {})
            column = None
        elif _tableEnd.match(line):
            m = _engineLine.match(line)
            if m is not None:
                mysql_engine_name = m.group(2)
                table["engine"] = mysql_engine_name
            table = None
        elif table is not None:  # process columns for given table
            m = _columnLine.match(line)
            if m is not None:
                first_token = m.group(1)
                if _isIndexDefinition(first_token):
                    t = "-"
                    if first_token == "PRIMARY":
                        t = "PRIMARY KEY"
                    elif first_token == "UNIQUE":
                        t = "UNIQUE"
                    idx_info = {
                        "type": t,
                        "columns": _retrIdxColumns(line)
                        }
                    table.setdefault("indexes", []).append(idx_info)
                else:
                    datatype, arraysize = _retrType(line)
                    datatype = MYSQL_TYPE_MAP[datatype.upper()]
                    if datatype == "boolean":
                        arraysize = None
                    column = {
                        "name": first_token,
                        "datatype": datatype,
                        "arraysize": arraysize,
                        "nullable": not _retrIsNotNull(line),
                    }
                    dv = _retrDefaultValue(line)
                    if dv is not None:
                        column["defaultValue"] = dv
                    if "columns" not in table:
                        table["columns"] = []
                    table["columns"].append(column)
            elif _isCommentLine(line):  # handle comments

                if column is None:
                    # table comment
                    if _containsDescrTagStart(line):
                        if _containsDescrTagEnd(line):
                            table["description"] = _retrDescr(line)
                        else:
                            table["description"] = _retrDescrStart(line)
                    elif "description" in table:
                        if _containsDescrTagEnd(line):
                            table["description"] += _retrDescrEnd(line)
                        else:
                            table["description"] += _retrDescrMid(line)
                else:
                    # column comment
                    if _containsDescrTagStart(line):
                        if _containsDescrTagEnd(line):
                            column["description"] = _retrDescr(line)
                        else:
                            column["description"] = _retrDescrStart(line)
                            column_description = 1
                    elif column_description:
                        if _containsDescrTagEnd(line):
                            column["description"] += _retrDescrEnd(line)
                            column_description = None
                        else:
                            column["description"] += _retrDescrMid(line)

                    # units
                    if _isUnitLine(line):
                        column["unit"] = _retrUnit(line)

                    # ucds
                    if _isUcdLine(line):
                        column["ucd"] = _retrUcd(line)

    schema_file.close()
    return schema


def _isIndexDefinition(c):
    return c in ["PRIMARY", "KEY", "INDEX", "UNIQUE"]


def _isCommentLine(fragment):
    return _commentLine.match(fragment) is not None


def _isUnitLine(fragment):
    return _unitLine.search(fragment) is not None


def _isUcdLine(fragment):
    return _ucdLine.search(fragment) is not None


def _retrUnit(fragment):
    return _unitLine.search(fragment).group(1)


def _retrUcd(fragment):
    return _ucdLine.search(fragment).group(1)


def _containsDescrTagStart(fragment):
    return '<descr>' in fragment


def _containsDescrTagEnd(fragment):
    return '</descr>' in fragment


def _retrDescr(fragment):
    return _descrLine.search(fragment).group(1)


def _retrDescrStart(fragment):
    return _descrStart.search(fragment).group(1)


def _retrDescrMid(fragment):
    found = _descrMiddle.search(fragment)
    return found.group(1) if found else ""


def _retrDescrEnd(fragment):
    return _descrEnd.search(fragment).group(1).rstrip()


def _retrIsNotNull(fragment):
    return 'NOT NULL' in fragment


def _retrType(fragment):
    datatype = fragment.split()[1].rstrip(',')
    size = None
    match = _size_parameter.search(datatype)
    if match:
        datatype = datatype[:match.start()]
        if match.group(1):
            # ignore match.group(1) == 0
            size = int(match.group(1))
    return datatype, size


def _retrDefaultValue(fragment):
    if not _defaultLine.search(fragment):
        return None
    arr = fragment.split()
    returnNext = 0
    for a in arr:
        if returnNext:
            return a.rstrip(',')
        if a == 'DEFAULT':
            returnNext = 1


def _retrIdxColumns(fragment):
    colExprs = _idxCols.search(fragment).group(1).split(',')
    columns = [" ".join([word for word in expr.split()
                         if word not in ('ASC', 'DESC')]) for expr in colExprs]
    return ", ".join(columns)






###############################################################################
def print_parsed_schema():
    t = parse_schema('../cat/sql/baselineSchema.sql')
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(t)

#if __name__ == '__main__':
#    printIt()
