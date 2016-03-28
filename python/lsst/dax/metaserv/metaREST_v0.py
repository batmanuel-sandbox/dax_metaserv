# LSST Data Management System
# Copyright 2015 AURA/LSST.
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

@author  Jacek Becla, SLAC
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

SAFE_NAME_REGEX = r'[a-zA-Z0-9_]+$'
SAFE_SCHEMA_PATTERN = re.compile(SAFE_NAME_REGEX)
SAFE_TABLE_PATTERN = re.compile(SAFE_NAME_REGEX)

metaREST = Blueprint('metaREST', __name__, template_folder="templates")


@metaREST.route('/', methods=['GET'])
def getRoot():
    fmt = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    if fmt == 'text/html':
        return "LSST Metadata Service v0. See: <a href='db'>/db</a> and <a href='image'>/image</a>."
    return "LSST Metadata Service v0. See: /db and /image."


@metaREST.route('/db', methods=['GET'])
def getDb():
    '''Lists types of databases (that have at least one database).'''
    query = "SELECT DISTINCT lsstLevel FROM Repo WHERE repoType = 'db'"
    return _resultsOf(text(query), scalar=True)


@metaREST.route('/db/<string:lsstLevel>', methods=['GET'])
def getDbPerType(lsstLevel):
    '''Lists databases for a given type.'''
    query = "SELECT dbName FROM Repo JOIN DbRepo on (repoId=dbRepoId) WHERE lsstLevel = :lsstLevel"
    return _resultsOf(text(query), paramMap={"lsstLevel": lsstLevel})


@metaREST.route('/db/<string:lsstLevel>/<string:dbName>', methods=['GET'])
def getDbPerTypeDbName(lsstLevel, dbName):
    '''Retrieves information about one database.'''
    # We don't use lsstLevel here because db names are unique across all types.
    query = "SELECT Repo.*, DbRepo.* " \
            "FROM Repo JOIN DbRepo on (repoId=dbRepoId) WHERE dbName = :dbName"
    return _resultsOf(text(query), paramMap={"dbName": dbName}, scalar=True)


@metaREST.route('/db/<string:lsstLevel>/<string:dbName>/tables', methods=['GET'])
def getDbPerTypeDbNameTables(lsstLevel, dbName):
    '''Lists table names in a given database.'''
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema=:dbName"
    return _resultsOf(text(query), paramMap={"dbName": dbName})


@metaREST.route('/db/<string:lsstLevel>/<string:dbName>/tables/'
                '<string:tableName>', methods=['GET'])
def getDbPerTypeDbNameTablesTableName(lsstLevel, dbName, tableName):
    '''Retrieves information about a table from a given database.'''
    query = "SELECT DDT_Table.* FROM DDT_Table " \
            "JOIN DbRepo USING (dbRepoId) " \
            "WHERE dbName=:dbName AND tableName=:tableName"
    return _resultsOf(text(query), paramMap={"dbName": dbName, "tableName": tableName}, scalar=True)


@metaREST.route('/db/<string:lsstLevel>/<string:dbName>/' +
                'tables/<string:tableName>/schema', methods=['GET'])
def getDbPerTypeDbNameTablesTableNameSchema(lsstLevel, dbName, tableName):
    '''Retrieves schema for a given table.'''
    # Scalar
    if SAFE_SCHEMA_PATTERN.match(dbName) and SAFE_TABLE_PATTERN.match(tableName):
        query = "SHOW CREATE TABLE %s.%s" % (dbName, tableName)
        return _resultsOf(query, scalar=True)
    return _response(_error("ValueError", "Database name or Table name is not safe"), 400)


@metaREST.route('/image', methods=['GET'])
def getImage():
    return "meta/.../image not implemented. I am supposed to print list of " \
           "supported image types here, something like: raw, template, coadd, " \
           "jpeg, calexp, ... etc"


_error = lambda exception, message: {"exception": exception, "message": message}
_vector = lambda results: {"results": results}
_scalar = lambda result: {"result": result}


def _resultsOf(query, paramMap=None, scalar=False):
    status_code = OK
    paramMap = paramMap or {}
    try:
        engine = current_app.config["default_engine"]
        if scalar:
            result = list(engine.execute(query, **paramMap).first())
            response = _scalar(result)
        else:
            results = [list(result) for result in engine.execute(query, **paramMap)]
            response = _vector(results)
    except SQLAlchemyError as e:
        log.debug("Encountered an error processing request: '%s'" % e.message)
        status_code = INTERNAL_SERVER_ERROR
        response = _error(type(e).__name__, e.message)
    return _response(response, status_code)


def _response(response, status_code):
    fmt = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    if fmt == 'text/html':
        response = render_response(response=response, status_code=status_code)
    else:
        response = json.dumps(response)
    return make_response(response, status_code)
