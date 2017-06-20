# LSST Data Management System
# Copyright 2017 AURA/LSST.
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
This module implements the RESTful interface for Metadata Service.
Corresponding URI: /meta. Default output format is json. Currently
supported formats: json and html.

@author Brian Van Klaveren, SLAC
"""
from collections import OrderedDict

from flask import Blueprint, request, current_app, make_response, jsonify

from http.client import OK, NOT_FOUND, INTERNAL_SERVER_ERROR
import logging as log
import re
from sqlalchemy import text, or_
from sqlalchemy.exc import SQLAlchemyError
from .model import session_maker, MSDatabase, MSDatabaseTable
from .api_model import *

SAFE_NAME_REGEX = r'[A-Za-z_$][A-Za-z0-9_$]*$'
SAFE_SCHEMA_PATTERN = re.compile(SAFE_NAME_REGEX)
SAFE_TABLE_PATTERN = re.compile(SAFE_NAME_REGEX)
ACCEPT_TYPES = ['application/json', 'text/html']

metaserv_api_v1 = Blueprint('metaserv_v1', __name__,
                            template_folder="templates")

Session = session_maker(current_app.config["default_engine"])


@metaserv_api_v1.route('/', methods=['GET'])
def root():
    fmt = request.accept_mimetypes.best_match(ACCEPT_TYPES)
    if fmt == 'text/html':
        return "LSST Metadata Service v1. " \
               "See: <a href='db'>/db</a>"
    return '{"links": "/db"}'


@metaserv_api_v1.route('/db', methods=['GET'])
def list_databases():
    """List databases known to this service.

    This simply returns a list of all known catalogs
    (logical databases).

    **Example request**
    .. code-block:: http
        GET /db HTTP/1.1
        Accept: application/json
        Accept-Encoding: gzip, deflate
        Connection: keep-alive
        Host: localhost:5000
        User-Agent: python-requests/2.13.0

    **Example response**
    .. code-block:: http
        HTTP/1.1 200 OK
        Content-Type: application/json
        Server: Werkzeug/0.11.3 Python/2.7.10

        {
            "results": [
               "S12_sdss",
               "qa_l2",
               "l1_dev"
            ]
        }

    :statuscode 200: No Error
    """
    db = Session()
    db_schema = Database(many=True)
    databases = db.query(MSDatabase).all()
    results = db_schema.dump(databases)
    return jsonify({"results": results.data})


@metaserv_api_v1.route('/db/<string:db_id>', methods=['GET'])
def database_info(db_id):
    """Show information about a particular database.

    This method will return general information about a catalog, as
    referred to by it's (`db_id`), including the default schema.

    A database identifier will always conform to the following regular
    expression:

        [A-Za-z_$][A-Za-z0-9_$]*

    **Example request**
    .. code-block:: http
        GET /db/S12_sdss HTTP/1.1
        Accept: application/json
        Accept-Encoding: gzip, deflate
        Connection: keep-alive
        Host: localhost:5000
        User-Agent: python-requests/2.13.0

    **Example response**
    .. code-block:: http
        HTTP/1.1 200 OK
        Content-Type: application/json
        Server: Werkzeug/0.11.3 Python/2.7.10

        {
            "result":
                {
                    "name":"S12_sdss",
                    "host": "lsst-qserv-dax01",
                    "port": "3360",
                    "default_schema":"sdss_stripe82_00"
                }
        }

    :param db_id: Database identifier

    :statuscode 200: No Error
    :statuscode 404: No database with that db_id found.
    """
    session = Session()
    database = session.query(MSDatabase).filter_by(
        or_(MSDatabase.db_id == db_id, MSDatabase.name == db_id)).first()
    db_schema = Database()
    schemas_schema = DatabaseSchema(many=True)
    db_result = db_schema.dump(database)
    schemas_result = schemas_schema.dump(database.schemas)
    response = OrderedDict(db_result.data)
    response["schemas"] = schemas_result.data
    return jsonify(response)


@metaserv_api_v1.route('/db/<string:db_id>/tables', methods=['GET'])
def schema_tables(db_id):
    """Show tables for the databases's default schema.

    This method returns a list of the tables and views for the default
    database schema. If no parameter is supplied, this will only return
    a simple list of the object names. If a query parameter


    **Example request 1**
    .. code-block:: http
        GET /db/S12_sdss/tables HTTP/1.1
        Accept: application/json
        Accept-Encoding: gzip, deflate
        Connection: keep-alive
        Host: localhost:5000
        User-Agent: python-requests/2.13.0

    **Example response 1**
    .. code-block:: http
        HTTP/1.1 200 OK
        Content-Type: application/json
        Server: Werkzeug/0.11.3 Python/2.7.10

        {
            "results": [
                "DeepCoadd", "DeepCoadd_Metadata",
                "DeepCoadd_To_Htm10", "Filter", ...
            ]
        }

    **Example request 2**
    .. code-block:: http
        GET /db/S12_sdss/tables?description=true HTTP/1.1
        Accept: application/json
        Accept-Encoding: gzip, deflate
        Connection: keep-alive
        Host: localhost:5000
        User-Agent: python-requests/2.13.0

    **Example response 2**
    .. code-block:: http
        HTTP/1.1 200 OK
        Content-Type: application/json
        Server: Werkzeug/0.11.3 Python/2.7.10

        {
            "results": [
              { "name": "DeepCoadd",
                "table_type": "table",
                "columns": [
                  { "name": "deepCoaddId",
                    "description": "Primary key (unique identifier).",
                    "utype": "int"
                    "ucd": "meta.id;src"
                  },
                  { "name": "ra",
                    "description": "RA of mean source cluster posi...",
                    "utype": "double",
                    "ucd": "pos.eq.ra",
                    "unit": "deg"
                  },
                  { "name": "decl",
                    "description": "Dec of mean source cluster pos...",
                    "utype": "double",
                    "ucd": "pos.eq.ra",
                    "unit": "deg"
                  },
                  ...,
                ]
              },
              ...,
              { "name": "Object",
                "description": "The Object table contains descript...",
                "table_type": "table",
                "columns": [
                  { "name": "objectId",
                    "description": "Unique object id.",
                    "utype": "int"
                    "ucd": "meta.id;src"
                  },
                  { "name": "ra",
                    "description": "RA of mean source cluster posi...",
                    "utype": "double",
                    "ucd": "pos.eq.ra",
                    "unit": "deg"
                  },
                  { "name": "decl",
                    "description": "Dec of mean source cluster pos...",
                    "utype": "double",
                    "ucd": "pos.eq.ra",
                    "unit": "deg"
                  },
                  ...
            ]
        }

    :param db_id: Database identifier
    :query boolean description: If true, show the expanded description
    in the response, including the columns of the tables.

    :statuscode 200: No Error
    :statuscode 404: No database with that db_id found.
    """
    session = Session()
    # This sends out 3 queries. It could be optimized into one large
    # Join query.
    database = session.query(MSDatabase).filter_by(
        or_(MSDatabase.db_id == db_id, MSDatabase.name == db_id)).first()
    tables = database.default_schema.tables
    table_schema = DatabaseTable(many=True)
    tables_result = table_schema.dump(tables)
    return jsonify({"results": tables_result.data})

@metaserv_api_v1.route('/db/<string:db_id>/tables/<string:table_name>',
                       methods=['GET'])
def database_schema_tables(db_id, table_name):
    """Show information about the table.

    This method returns a list of the tables for the default database
    schema. If no parameter is supplied, this will only return a
    simple list of the table names. If a query parameter


    **Example request**
    .. code-block:: http
        GET /db/S12_sdss/tables/Object HTTP/1.1
        Accept: application/json
        Accept-Encoding: gzip, deflate
        Connection: keep-alive
        Host: localhost:5000
        User-Agent: python-requests/2.13.0

    **Example response**
    .. code-block:: http
       HTTP/1.1 200 OK
       Content-Type: application/json
       Server: Werkzeug/0.11.3 Python/2.7.10

        {
            "result": {
                "name": "Object",
                "description": "The Object table contains descript...",
                "columns": [
                  { "name": "objectId",
                    "description": "Unique object id.",
                    "datatype": "int"
                    "ucd": "meta.id;src"
                  },
                  { "name": "ra",
                    "description": "RA of mean source cluster posi...",
                    "datatype": "double",
                    "ucd": "pos.eq.ra",
                    "unit": "deg"
                  },
                  { "name": "decl",
                    "description": "Dec of mean source cluster pos...",
                    "datatype": "double",
                    "ucd": "pos.eq.ra",
                    "unit": "deg"
                  },
                  ...
            }
        }

    :param db_id: Database identifier
    :param table_name: Name of table or view
    :query description: If supplied, must be one of the following:
       `content`
    in the response, including the columns of the tables.

    :statuscode 200: No Error
    :statuscode 404: No database with that db_id found.
    """

    session = Session()
    # This sends out 3 queries. It could be optimized into one large
    # Join query.
    database = session.query(MSDatabase).filter_by(
        or_(MSDatabase.db_id == db_id, MSDatabase.name == db_id)).first()
    table = database.default_schema.filter_by(
        MSDatabaseTable.name == table_name)
    table_schema = DatabaseTable()
    tables_result = table_schema.dump(table)
    return jsonify({"result": tables_result.data})
