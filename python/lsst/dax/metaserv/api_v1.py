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

from flask import Blueprint, request, current_app, g, jsonify

from http.client import OK, NOT_FOUND, INTERNAL_SERVER_ERROR
import logging as log
import re
from sqlalchemy import text, or_, and_
from sqlalchemy.exc import SQLAlchemyError
from .model import session_maker, MSDatabase, MSDatabaseSchema, MSDatabaseTable
from .api_model import *

SAFE_NAME_REGEX = r'[A-Za-z_$][A-Za-z0-9_$]*$'
SAFE_SCHEMA_PATTERN = re.compile(SAFE_NAME_REGEX)
SAFE_TABLE_PATTERN = re.compile(SAFE_NAME_REGEX)
ACCEPT_TYPES = ['application/json', 'text/html']

metaserv_api_v1 = Blueprint('metaserv_v1', __name__,
                            template_folder="templates")


def Session():
    db = getattr(g, '_Session', None)
    if db is None:
        db = g._session = session_maker(current_app.config["default_engine"])
    return db()


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


@metaserv_api_v1.route('/db/<string:db_id>/', methods=['GET'])
def database(db_id):
    """Show information about a particular database.

    This method will return general information about a catalog, as
    referred to by it's (``id``), including the default schema.

    A database identifier will always conform to the following regular
    expression:

        ``[A-Za-z_$][A-Za-z0-9_$]*``

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
    :statuscode 404: No database with that id found.
    """
    session = Session()
    database = session.query(MSDatabase).filter(
        or_(MSDatabase.id == db_id, MSDatabase.name == db_id)).first()
    request.database = database
    db_schema = Database()
    schemas_schema = DatabaseSchema(many=True)
    db_result = db_schema.dump(database)
    schemas_result = schemas_schema.dump(database.schemas)
    response = OrderedDict(db_result.data)
    response["schemas"] = schemas_result.data
    return jsonify({"results": response})


@metaserv_api_v1.route('/db/<string:db_id>/<string:schema_id>/tables/',
                       methods=['GET'])
@metaserv_api_v1.route('/db/<string:db_id>/tables/', methods=['GET'])
def tables(db_id, schema_id=None):
    """Show tables for the databases's default schema.

    This method returns a list of the tables and views for the default
    database schema. If no parameter is supplied, this will only return
    a simple list of the object names. If a query parameter

    **Example request 1**

    .. code-block:: http

        GET /db/S12_sdss/tables?description=true HTTP/1.1
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
              { "name": "DeepCoadd",
                "table_type": "table",
                "columns": [
                  { "name": "deepCoaddId",
                    "description": "Primary key (unique identifier).",
                    "utype": "int",
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
                  "..."
                ]
              },
              "..."
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
                  "..."
            ]
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
        or_(MSDatabase.id == db_id, MSDatabase.name == db_id)).first()
    request.database = database

    if schema_id is not None:
        schema = database.schemas.filter(or_(
            MSDatabaseSchema.id == schema_id,
            MSDatabaseSchema.name == schema_id
        )).scalar()
    else:
        schema = database.default_schema.scalar()

    schema_schema = DatabaseSchema()
    schema_result = schema_schema.dump(schema)
    tables = session.query(MSDatabaseTable).filter(
        MSDatabaseTable.schema_id == schema.id).all()
    table_schema = DatabaseTable(many=True)
    tables_result = table_schema.dump(tables)
    return jsonify({"results": {
        "schema": schema_result.data,
        "tables": tables_result.data}
    })


@metaserv_api_v1.route('/db/<string:db_id>/<string:schema_id>/tables/'
                       '<table_id>/',
                       methods=['GET'])
@metaserv_api_v1.route('/db/<string:db_id>/tables/<table_id>/',
                       methods=['GET'])
def table(db_id, table_id, schema_id=None):
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
                    "datatype": "int",
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
                  "..."
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
        or_(MSDatabase.id == db_id, MSDatabase.name == db_id)).scalar()
    request.database = database
    if schema_id is not None:
        schema = database.schemas.filter(or_(
            MSDatabaseSchema.id == schema_id,
            MSDatabaseSchema.name == schema_id
        )).scalar()
    else:
        schema = database.default_schema.scalar()

    table = session.query(MSDatabaseTable).filter(and_(
        MSDatabaseTable.schema_id == schema.id,
        or_(
            MSDatabaseTable.name == table_id,
            MSDatabaseTable.id == table_id)
        )
    ).scalar()

    table_schema = DatabaseTable()
    tables_result = table_schema.dump(table)
    return jsonify({"result:": tables_result.data})
