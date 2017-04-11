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

from flask import Blueprint, request, current_app, make_response
from lsst.dax.webservcommon import render_response

from httplib import OK, NOT_FOUND, INTERNAL_SERVER_ERROR
import json
import logging as log
import re
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

SAFE_NAME_REGEX = r'[A-Za-z_$][A-Za-z0-9_$]*$'
SAFE_SCHEMA_PATTERN = re.compile(SAFE_NAME_REGEX)
SAFE_TABLE_PATTERN = re.compile(SAFE_NAME_REGEX)
ACCEPT_TYPES = ['application/json', 'text/html']

metaserv_api_v1 = Blueprint('metaserv_v1', __name__,
                            template_folder="templates")


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
    query = """SELECT dbName as "name", connHost as "host", connPort as "port", """ \
            """defaultSchema as "default_schema" FROM DbRepo"""
    return _results_of(text(query))


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
    query = """SELECT dbName as "name", connHost as "host", connPort as "port",
               defaultSchema as "default_schema" FROM DbRepo WHERE dbName = :db_id"""
    return _results_of(text(query), param_map={"db_id": db_id}, scalar=True)


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
    query = """SELECT tableName as "name", schemaName as "schema_name",
                'table' as "table_type", tables.descr as "description"
            FROM DbRepo repo
            JOIN DDT_Table tables USING (dbRepoId)
            WHERE repo.dbName = :db_id and tables.schemaName = repo.defaultSchemaName"""
    return _results_of(text(query), param_map={"db_id": db_id})


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

    query = """SELECT tableName as "name", schemaName as "schema_name",
                'table' as "table_type", tables.descr as "description"
            FROM DbRepo repo
            JOIN DDT_Table tables USING (dbRepoId)
            JOIN DDT_Column columns USING (tableId)
            WHERE repo.dbName = :db_id and tables.schemaName = repo.defaultSchemaName"""
    return _results_of(text(query), param_map={"db_id": db_id})


_error = lambda exception, message: {"exception": exception, "message": message}
_vector = lambda results: {"results": results}
_scalar = lambda result: {"result": result}


def _results_of(query, param_map=None, scalar=False):
    return _raw_results_of(query, param_map, scalar)


def _raw_results_of(query, param_map=None, scalar=False):
    status_code = OK
    param_map = param_map or {}
    try:
        engine = current_app.config["default_engine"]
        if scalar:
            result = engine.execute(query, **param_map).first()
            response = dict(result=dict(result))
        else:
            results = [dict(result) for result in engine.execute(query, **param_map)]
            response = _vector(results)
    except SQLAlchemyError as e:
        log.debug("Encountered an error processing request: '%s'" % e.message)
        status_code = INTERNAL_SERVER_ERROR
        response = _error(type(e).__name__, e.message)
    return response, status_code


def _response(response, status_code):
    fmt = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    if fmt == 'text/html':
        response = render_response(response=response, status_code=status_code)
    else:
        response = json.dumps(response)
    return make_response(response, status_code)
