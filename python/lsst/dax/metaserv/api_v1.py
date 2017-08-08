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

from flask import Blueprint, current_app, g, jsonify

from http.client import OK, NOT_FOUND, INTERNAL_SERVER_ERROR
import logging as log
import re
from sqlalchemy import or_, and_
import traceback

from .model import session_maker, MSDatabase, MSDatabaseSchema, MSDatabaseTable
from .api_model import *

SAFE_NAME_PATTERN = re.compile(r'[A-Za-z_$][A-Za-z0-9_$]*$')
ACCEPT_TYPES = ['application/json', 'text/html']

metaserv_api_v1 = Blueprint('metaserv_v1', __name__,
                            template_folder="templates")


def Session():
    db = getattr(g, '_Session', None)
    if db is None:
        db = g._session = session_maker(current_app.config["default_engine"])
    return db()

class ResourceNotFoundError(Exception):
    pass


@metaserv_api_v1.errorhandler(ResourceNotFoundError)
def handle_missing_resource(error):
    err = {
        "exception": "ResourceNotFound",
        "message": error.args[0]
    }

    if len(error.args) > 1:
        err["more"] = [str(arg) for arg in error.args[1:]]
    response = jsonify(err)
    response.status_code = NOT_FOUND
    return response


@metaserv_api_v1.errorhandler(Exception)
def handle_unhandled_exceptions(error):
    log.error("Error handling request:\n {}".format(error))
    log.error(traceback.format_exc())
    err = {
        "exception": error.__class__.__name__,
        "message": error.args[0]
    }

    if len(error.args) > 1:
        err["more"] = [str(arg) for arg in error.args[1:]]
    response = jsonify(err)
    response.status_code = INTERNAL_SERVER_ERROR
    return response


@metaserv_api_v1.route('/', methods=['GET'])
def root():
    fmt = request.accept_mimetypes.best_match(ACCEPT_TYPES)
    if fmt == 'text/html':
        return "LSST Metadata Service v1. " \
               "See: <a href='db'>/db</a>"
    return '{"links": "/db"}'


@metaserv_api_v1.route('/db/', methods=['GET'])
def databases():
    """List databases known to this service.

    A database is a catalog, or set, of tables organized under one or
    more schemas.

    A database has a default schema.

    We currently assume that all databases map to exactly one host,
    and we include information on how to connect to that host.

    All results in the response include this information about a
    database, in addition to a URL which allows a user to query
    more information about a database and it's schema(s).

    **Example request**

    .. code-block:: http

        GET /meta/v1/db/ HTTP/1.1
        User-Agent: curl/7.29.0
        Host: example.com
        Accept: */*


    **Example response**

    .. code-block:: http

        HTTP/1.1 200 OK
        Server: nginx/1.10.3 (Ubuntu)
        Date: Tue, 08 Aug 2017 01:17:46 GMT
        Content-Type: application/json
        Content-Length: 258
        Connection: keep-alive

        {
          "results": [
            {
              "name": "W13_sdss",
              "id": 1,
              "url": "http://example.com/meta/v1/db/1/",
              "host": "lsst-db01",
              "port": 4040,
              "default_schema": "sdss_stripe82_00"
            }
          ]
        }


    :statuscode 200: No Error
    """
    db = Session()
    db_schema = Database(many=True)
    databases = db.query(MSDatabase).all()
    results = db_schema.dump(databases)
    return jsonify({"results": results.data})


@metaserv_api_v1.route('/db/<string:db_id>/', methods=['GET'])
def database(db_id):
    """Show information about a particular database.

    This method will return general information about a catalog, as
    referred to by it's ``db_id``. A ``db_id`` may either be the name
    of the database, which can change, or an integer id number, which
    is guaranteed to be preserved across potential name changes.

    In the case of a named database, the named database will always
    conform to the following regular expression:

        ``[A-Za-z_$][A-Za-z0-9_$]*``

    **Example request 1, using named id**

    .. code-block:: http

        GET /meta/v1/db/W13_sdss/ HTTP/1.1
        User-Agent: curl/7.29.0
        Host: example.com
        Accept: */*

    **Equivalent request, using integer id**

    .. code-block:: http

        GET /meta/v1/db/1/ HTTP/1.1
        User-Agent: curl/7.29.0
        Host: example.com
        Accept: */*


    **Example response**

    .. code-block:: http

        HTTP/1.1 200 OK
        Server: nginx/1.10.3 (Ubuntu)
        Date: Tue, 08 Aug 2017 01:20:27 GMT
        Content-Type: application/json
        Content-Length: 441
        Connection: keep-alive

        {
          "result": {
              "name": "W13_sdss",
              "id": 1,
              "url": "http://example.com/meta/v1/db/W13_sdss/",
              "host": "lsst-db01",
              "port": 4040,
              "default_schema": "sdss_stripe82_00",
              "schemas": [
                {
                  "name": "sdss_stripe82_00",
                  "id": 1,
                  "url": "http://example.com/meta/v1/db/1/1/tables/",
                  "description": null,
                  "is_default_schema": true
                }
              ]
            }
        }

    :param db_id: Database identifier

    :statuscode 200: No Error
    :statuscode 404: No database with that found.
    """
    session = Session()
    database = session.query(MSDatabase).filter(
        _filter_for_id(MSDatabase, db_id)).first()

    if not database:
        raise ResourceNotFoundError("No Database with id {}".format(db_id))

    request.database = database
    db_schema = Database()
    schemas_schema = DatabaseSchema(many=True)
    db_result = db_schema.dump(database)
    schemas_result = schemas_schema.dump(database.schemas)
    response = OrderedDict(db_result.data)
    response["schemas"] = schemas_result.data
    return jsonify({"result": response})


@metaserv_api_v1.route('/db/<string:db_id>/tables/', methods=['GET'])
@metaserv_api_v1.route('/db/<string:db_id>/<string:schema_id>/tables/',
                       methods=['GET'])
def tables(db_id, schema_id=None):
    """Show tables for the databases's default schema.

    This method returns a list of the tables and views for the default
    database schema. If no parameter is supplied, this will only return
    a simple list of the object names. If a query parameter

    **Example request 1**

    .. code-block:: http

        GET /meta/v1/db/W13_sdss/tables/ HTTP/1.1
        User-Agent: curl/7.29.0
        Host: example.com
        Accept: */*

    **Example response 1**

    .. code-block:: http

        HTTP/1.1 200 OK
        Content-Type: application/json
        Server: Werkzeug/0.11.3 Python/2.7.10

        {
          "result": {
            "schema": {
              "name": "sdss_stripe82_00",
              "id": 1,
              "url": "http://example.com/meta/v1/db/1/1/tables/",
              "description": null,
              "is_default_schema": true
            },
            "tables": [
              "...",
              {
                "name": "DeepCoadd",
                "id": 4,
                "url": "http://example.com/meta/v1/db/1/tables/4/",
                "description": "Not filled.",
                "columns": [
                  {
                    "name": "deepCoaddId",
                    "id": 15,
                    "description": "Primary key (unique identifier).",
                    "ordinal": 0,
                    "ucd": "meta.id;obs.image",
                    "unit": "",
                    "datatype": "long",
                    "nullable": false,
                    "arraysize": null
                  },
                  {
                    "name": "tract",
                    "id": 16,
                    "description": "Sky-tract number.",
                    "ordinal": 1,
                    "ucd": "",
                    "unit": "",
                    "datatype": "int",
                    "nullable": false,
                    "arraysize": null
                  },
                  "..."
                ]
              },
              "...",
              {
                "name": "DeepSource",
                "id": 9,
                "url": "http://example.com/meta/v1/db/1/tables/9/",
                "description": "Not filled. Table to store high si...",
                "columns": [
                  {
                    "name": "deepSourceId",
                    "id": 121,
                    "description": "Primary key (unique identifier)",
                    "ordinal": 0,
                    "ucd": "meta.id;src",
                    "unit": "",
                    "datatype": "long",
                    "nullable": false,
                    "arraysize": null
                  },
                  {
                    "name": "parentDeepSourceId",
                    "id": 122,
                    "description": "deepSourceId of parent if sour...",
                    "ordinal": 1,
                    "ucd": "meta.id.parent;src",
                    "unit": "",
                    "datatype": "long",
                    "nullable": true,
                    "arraysize": null
                  },
                  "..."
                ]
              }
            ]
          }
        }

    :param db_id: Database identifier
    :param schema_id: Name or ID of the schema. If none, use default.

    :statuscode 200: No Error
    :statuscode 404: No database with that id found.
    """
    session = Session()
    # This sends out 3 queries. It could be optimized into one large
    # Join query.

    database = session.query(MSDatabase).filter(
        _filter_for_id(MSDatabase, db_id)).first()

    if not database:
        raise ResourceNotFoundError("No Database with id {}".format(db_id))

    request.database = database

    if schema_id is not None:
        schema = database.schemas.filter(
            _filter_for_id(MSDatabaseSchema, schema_id)
        ).scalar()
    else:
        schema = database.default_schema.scalar()

    if not schema:
        raise ResourceNotFoundError("No Schema with id {}".format(schema_id))

    schema_schema = DatabaseSchema()
    schema_result = schema_schema.dump(schema)
    tables = session.query(MSDatabaseTable).filter(
        MSDatabaseTable.schema_id == schema.id).all()
    table_schema = DatabaseTable(many=True)
    tables_result = table_schema.dump(tables)
    return jsonify({"result": {
        "schema": schema_result.data,
        "tables": tables_result.data}
    })


@metaserv_api_v1.route('/db/<string:db_id>/tables/<table_id>/',
                       methods=['GET'])
@metaserv_api_v1.route('/db/<string:db_id>/<string:schema_id>/tables/'
                       '<table_id>/',
                       methods=['GET'])
def table(db_id, table_id, schema_id=None):
    """Show information about the table.

    This method returns a list of the tables for the default database
    schema. If no parameter is supplied, this will only return a
    simple list of the table names. If a query parameter


    **Example request**

    .. code-block:: http

        GET /meta/v1/db/W13_sdss/tables/DeepSource/ HTTP/1.1
        User-Agent: curl/7.29.0
        Host: example.com
        Accept: */*

    **Example response**

    .. code-block:: http

        HTTP/1.1 200 OK
        Server: nginx/1.10.3 (Ubuntu)
        Date: Tue, 08 Aug 2017 02:45:11 GMT
        Content-Type: application/json

        {
          "result:": {
            "name": "DeepSource",
            "id": 9,
            "url": "http://example.com/meta/v1/db/1/tables/9/",
            "description": "Not filled. Table to store high signal...",
            "columns": [
              {
                "name": "deepSourceId",
                "id": 121,
                "description": "Primary key (unique identifier)",
                "ordinal": 0,
                "ucd": "meta.id;src",
                "unit": "",
                "datatype": "long",
                "nullable": false,
                "arraysize": null
              },
              {
                "name": "parentDeepSourceId",
                "id": 122,
                "description": "deepSourceId of parent if source i...",
                "ordinal": 1,
                "ucd": "meta.id.parent;src",
                "unit": "",
                "datatype": "long",
                "nullable": true,
                "arraysize": null
              },
              {
                "name": "deepCoaddId",
                "id": 123,
                "description": "ID of the coadd the source was det...",
                "ordinal": 2,
                "ucd": "meta.id;obs.image",
                "unit": "",
                "datatype": "long",
                "nullable": false,
                "arraysize": null
              },
              {
                "name": "filterId",
                "id": 124,
                "description": "ID of filter used for the coadd th...",
                "ordinal": 3,
                "ucd": "meta.id;instr.filter",
                "unit": "",
                "datatype": "short",
                "nullable": false,
                "arraysize": null
              },
              {
                "name": "ra",
                "id": 125,
                "description": "ICRS RA of source centroid (x, y).",
                "ordinal": 4,
                "ucd": "pos.eq.ra",
                "unit": "deg",
                "datatype": "double",
                "nullable": false,
                "arraysize": null
              },
              {
                "name": "decl",
                "id": 126,
                "description": "ICRS Dec of source centroid (x, y).",
                "ordinal": 5,
                "ucd": "pos.eq.dec",
                "unit": "deg",
                "datatype": "double",
                "nullable": false,
                "arraysize": null
              },
              "...",
            ]
          }
        }

    :param db_id: Database identifier
    :param table_id: Name of table or view
    :param schema_id: Name or ID of the schema. If none, use default.
    :query description: If supplied, must be one of the following:
       `content`
    in the response, including the columns of the tables.

    :statuscode 200: No Error
    :statuscode 404: No database with that id found.
    """
    session = Session()
    # This sends out 3 queries. It could be optimized into one large
    # Join query.
    database = session.query(MSDatabase).filter(
        _filter_for_id(MSDatabase, db_id)
    ).scalar()

    if not database:
        raise ResourceNotFoundError("No Database with id {}".format(db_id))

    request.database = database
    if schema_id is not None:
        schema = database.schemas.filter(
            _filter_for_id(MSDatabaseSchema, schema_id)
        ).scalar()
    else:
        schema = database.default_schema.scalar()

    if not schema:
        raise ResourceNotFoundError("No Schema with id {}".format(schema_id))

    table = session.query(MSDatabaseTable).filter(and_(
        MSDatabaseTable.schema_id == schema.id,
        _filter_for_id(MSDatabaseTable, table_id))
    ).scalar()

    if not table:
        raise ResourceNotFoundError("No Table with id {}".format(table_id))

    table_schema = DatabaseTable()
    tables_result = table_schema.dump(table)
    return jsonify({"result": tables_result.data})


def _filter_for_id(table, id):
    column_key = "name" if SAFE_NAME_PATTERN.match(id) else "id"
    return table.columns[column_key] == id
