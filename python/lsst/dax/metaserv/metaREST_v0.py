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

from httplib import OK, INTERNAL_SERVER_ERROR
import json
import logging as log
import re
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.inspection import inspect


SAFE_NAME_REGEX = r'[a-zA-Z0-9_]+$'
SAFE_SCHEMA_PATTERN = re.compile(SAFE_NAME_REGEX)
SAFE_TABLE_PATTERN = re.compile(SAFE_NAME_REGEX)

metaREST = Blueprint('metaREST', __name__, template_folder="templates")


@metaREST.route('/', methods=['GET'])
def root():
    fmt = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    if fmt == 'text/html':
        return "LSST Metadata Service v0. See: <a href='db'>/db</a> and <a href='image'>/image</a>."
    return "LSST Metadata Service v0. See: /db and /image."


@metaREST.route('/db', methods=['GET'])
def levels():
    """Lists types of databases (that have at least one database)."""
    query = "SELECT DISTINCT lsstLevel FROM Repo WHERE repoType = 'db'"
    return _results_of(text(query), scalar=True)


@metaREST.route('/db/<string:lsst_level>', methods=['GET'])
def schemas_for(lsst_level):
    """Lists databases for a given type."""
    query = "SELECT dbName FROM Repo JOIN DbRepo on (repoId=dbRepoId) WHERE lsstLevel = :lsst_level"
    return _results_of(text(query), params={"lsst_level": lsst_level})


@metaREST.route('/db/<string:lsst_level>/<string:db_name>', methods=['GET'])
def show_repo_info(lsst_level, db_name):
    """Retrieves information about one database."""
    # We don't use lsst_level here because db names are unique across all types.
    query = "SELECT Repo.*, DbRepo.* " \
            "FROM Repo JOIN DbRepo on (repoId=dbRepoId) WHERE db_name = :db_name"
    return _results_of(text(query), params={"db_name": db_name}, scalar=True)


@metaREST.route('/db/<string:lsst_level>/<string:db_name>/tables', methods=['GET'])
def list_tables(lsst_level, db_name):
    """Lists table names in a given database."""
    engine = current_app.config["default_engine"]
    try:
        results = inspect(engine).get_table_names(schema=db_name)
        response = _response(dict(results=results), OK)
    except SQLAlchemyError as e:
        log.debug("Encountered an error processing request: '%s'" % e.message)
        response = _response(_error(type(e).__name__, e.message), INTERNAL_SERVER_ERROR)
    return response


@metaREST.route('/db/<string:lsst_level>/<string:db_name>/tables/'
                '<string:table_name>', methods=['GET'])
def table_info(lsst_level, db_name, table_name):
    """Retrieves information about a table from a given database."""
    query = "SELECT DDT_Table.* FROM DDT_Table " \
            "JOIN DbRepo USING (dbRepoId) " \
            "WHERE dbName=:db_name AND tableName=:table_name"
    return _results_of(text(query), params={"db_name": db_name, "table_name": table_name}, scalar=True)


@metaREST.route('/db/<string:lsst_level>/<string:db_name>/' +
                'tables/<string:table_name>/schema', methods=['GET'])
def table_schema(lsst_level, db_name, table_name):
    """Retrieves schema for a given table."""
    # Scalar
    if SAFE_SCHEMA_PATTERN.match(db_name) and SAFE_TABLE_PATTERN.match(table_name):
        query = "SHOW CREATE TABLE %s.%s" % (db_name, table_name)
        return _results_of(query, scalar=True)
    return _response(_error("ValueError", "Database name or Table name is not safe"), 400)


def _error(exception, message):
    return dict(exception=exception, message=message)


def _results_of(query, params=None, scalar=False):
    status_code = OK
    params = params or {}
    try:
        engine = current_app.config["default_engine"]
        if scalar:
            result = list(engine.execute(query, **params).first())
            response = dict(result=result)
        else:
            results = [list(result) for result in engine.execute(query, **params)]
            response = dict(results=results)
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
