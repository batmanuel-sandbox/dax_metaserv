#!/usr/bin/env python

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
Metadata Server admin program. It is currently used to ingest information into the
LSST Metadata Server.

@author  Jacek Becla, SLAC
"""

import logging as log

from lsst.db.engineFactory import getEngineFromFile
from .schemaToMeta import parse_schema
from .metaBException import MetaBException


class MetaAdminImpl(object):
    """
    Implements the guts of the metaserver admin program."
    """

    def __init__(self, metaserv_mysql_file):
        """
        :param metaserv_mysql_file: mysql auth file for metaserv db and metaserv user
        """
        # Create metaserv engine
        self.ms_engine = getEngineFromFile(metaserv_mysql_file)
        self._log = log.getLogger("lsst.metaserv.admin")

    def load_catalog(self, db_name, schema_name, schema_file, host, port,
                     schema_version, schema_description, level, data_release,
                     owner, accessibility, project_name, target_engine=None):
        """
        Add a database along with additional schema description
        provided through.
        :param db_name: database name
        :param schema_name: name of default schema associated with
        the schema file.
        :param schema_file: ascii file containing schema with
        description
        :param host: Hostname of this database
        :param port: Port the database is reachable on.
        :param schema_version: Version of the default schema.
        :param schema_description: Description of the default schema
        :param level: level (e.g., L1, L2, L3)
        :param data_release: Associated Data Release
        :param owner: owner of the database
        :param accessibility: accessibility of the database
        (pending/public/private).
        :param project_name: name of the project the db is associated
        with.
        :param target_engine: If not None, this engine will be used to
        check that metadata will be consistent with what's loaded
        in the target_engine's database.

        The function connects to two database servers:
        a) one that has the database that is being loaded
        b) one that has the metaserv database
        If they are both on the same server, the connection is reused.

        The course of action:
        * connect to the server that has database that is being loaded
        * parse the ascii schema file
        * fetch schema information from the information_schema
        * do the matching, add info fetched from information_schema to the
          in memory structure produced by parsing ascii schema file
        * fetch schema description and version (which is kept as data inside
          a special table in the database that is being loaded). Ignore if it
          does not exist.
        * Capture information from mysql auth file about connection information
        * connect to the metaserv database
        * validate owner, project (these must be loaded into metaserv prior to
        calling this function)
        * load all the information into metaserv in various tables (Repo,
          DDT_Table, DDT_Column)

        It raises following MetaBEXceptions:
        * DB_DOES_NOT_EXISTS if database dbName does not exist
        * NOT_MATCHING if the database schema and ascii schema don't match
        * TB_NOT_IN_DB if the table is described in ascii schema, but it is missing
                       in the database
        * COL_NOT_IN_TB if the column is described in ascii schema, but it is
                        missing in the database
        * COL_NOT_IN_FL if the column is in the database schema, but not in ascii
                        schema
        * Db object can throw various DbException and MySQL exceptions
        """

        # Parse the ascii schema file
        parsed_schema = parse_schema(schema_file)

        if target_engine:
            self._check_schema_consistency(
                db_name, schema_name, parsed_schema, schema_version,
                schema_description, target_engine)

        # Now, we will be talking to the metaserv database, so change
        # connection as needed
        conn = self.ms_engine.connect()

        # get ownerId, this serves as validation that this is a valid owner name
        ret = conn.execute("SELECT userId FROM User WHERE mysqlUserName = %s",
                           (owner,))

        if ret.rowcount != 1:
            self._log.error("Owner '%s' not found.", owner)
            raise MetaBException(MetaBException.OWNER_NOT_FOUND, owner)
        owner_id = ret.scalar()

        # get projectId, this serves as validation that this is a valid project name
        ret = conn.execute("SELECT projectId FROM Project "
                           "WHERE projectName = %s",
                           (project_name,))
        if ret.rowcount != 1:
            self._log.error("Project '%s' not found.", project_name)
            raise MetaBException(MetaBException.PROJECT_NOT_FOUND, project_name)
        project_id = ret.scalar()

        # Finally, save things in the MetaServ database
        cmd = "INSERT INTO Repo(url, projectId, repoType, lsstLevel, " \
              "dataRelease, version, shortName, description, ownerId, " \
              "accessibility) VALUES('/dummy',%s,'db',%s,%s,%s,%s,%s,%s,%s) "
        opts = (project_id, level, data_release, schema_version, db_name,
                schema_description, owner_id, accessibility)
        results = conn.execute(cmd, opts)
        repo_id = results.lastrowid
        cmd = "INSERT INTO DbRepo(dbRepoId, dbName, defaultSchema, " \
              "connHost, connPort) VALUES(%s,%s,%s,%s,%s)"
        conn.execute(cmd, (repo_id, db_name, schema_name, host, port))

        for table_name in parsed_schema:
            table = parsed_schema[table_name]
            cmd = 'INSERT INTO DDT_Table(dbRepoId, tableName, ' \
                  'schemaName, descr) VALUES(%s, %s, %s, %s)'
            results = conn.execute(cmd, (repo_id, table_name, schema_name,
                                         table.get("description", "")))
            table_id = results.lastrowid
            is_first = True
            columns = table["columns"]
            for col, ord_pos in zip(columns, range(len(columns))):
                if is_first:
                    cmd = 'INSERT INTO DDT_Column(columnName, tableId, ' \
                          'ordinalPosition, descr, ucd, units) VALUES '
                    opts = ()
                    is_first = False
                else:
                    cmd += ', '
                cmd += '(%s, %s, %s, %s, %s, %s)'
                opts += (col["name"], table_id, ord_pos+1,
                         col.get("description", ""), col.get("ucd", ""),
                         col.get("unit", ""))
            conn.execute(cmd, opts)

    def add_user(self, mysql_username, first_name, last_name, affiliation,
                 email):
        """
        Add user.

        :param mysql_username: MySQL user name
        :param first_name:  first name
        :param last_name:  last name
        :param affiliation:  short name of the affiliation
        (home institution)
        :param email:  email address
        """
        cmd = "SELECT instId FROM Institution WHERE instName = %s"
        inst_id = self.ms_engine.execute(cmd, (affiliation,)).scalar()
        if inst_id is None:
            raise MetaBException(MetaBException.INST_NOT_FOUND, affiliation)
        cmd = "INSERT INTO User(mysqlUserName, firstName, lastName, email, " \
              "instId) VALUES(%s, %s, %s, %s, %s)"
        self.ms_engine.execute(cmd, (mysql_username, first_name, last_name,
                                     email, inst_id))

    def add_institution(self, institution_name):
        """
        Add institution.
        :param institution_name:  the name
        """
        ret = self.ms_engine.execute(
            "SELECT COUNT(*) FROM Institution WHERE instName=%s",
            (institution_name,))
        if ret.scalar() == 1:
            raise MetaBException(MetaBException.INST_EXISTS, institution_name)
        self.ms_engine.execute(
            "INSERT INTO Institution(instName) VALUES(%s)",
            (institution_name,))

    def add_project(self, project_name):
        """
        Add project.

        :param project_name:  the name
        """
        ret = self.ms_engine.execute(
            "SELECT COUNT(*) FROM Project WHERE projectName=%s",
            (project_name,))
        if ret.scalar() == 1:
            raise MetaBException(MetaBException.PROJECT_EXISTS, project_name)
        self.ms_engine.execute(
            "INSERT INTO Project(projectName) VALUES(%s)", (project_name,))

    def _check_schema_consistency(self, db_name, schema_name, parsed_schema,
                                  schema_version, schema_description,
                                  target_engine):
        # Connect to the server that has database that is being added
        from sqlalchemy.engine.reflection import Inspector

        inspector = Inspector(target_engine)

        if schema_name not in inspector.get_schema_names():
            self._log.error("Schema '%s' not found.", db_name)
            raise MetaBException(MetaBException.DB_DOES_NOT_EXIST, db_name)

        db_tables = inspector.get_table_names(schema=schema_name)

        for table_name, parsed_table in parsed_schema.items():
            # Check parsed tables - we allow other tables in schema
            if table_name not in db_tables:
                self._log.error(
                    "Table '%s' not found in db, present in ascii file.",
                    table_name)
                raise MetaBException(MetaBException.TB_NOT_IN_DB, table_name)

            db_columns = inspector.get_columns(table_name=table_name,
                                               schema=schema_name)
            parsed_columns = parsed_table["columns"]
            if len(parsed_columns) != len(db_columns):
                self._log.error("Number of columns in db for table %s (%d) "
                                "differs from number columns in schema (%d)",
                                table_name, len(db_columns),
                                len(parsed_columns))
                raise MetaBException(MetaBException.NOT_MATCHING)

            for column in parsed_columns:
                column_name = column["name"]
                if column_name not in db_columns:
                    self._log.error(
                        "Column '%s.%s' not found in db, "
                        "but exists in schema DDL",
                        table_name, column_name)
                    raise MetaBException(MetaBException.COL_NOT_IN_TB,
                                         column_name, table_name)

        # Get schema description and version, it is ok if it is missing
        ret = target_engine.execute(
            "SELECT version, descr FROM %s.ZZZ_Schema_Description" % db_name)
        if ret.rowcount != 1:
            self._log.error(
                "Db '%s' does not contain schema version/description", db_name)
        else:
            (found_schema_version, found_schema_description) = ret.first()
            if found_schema_version != schema_version or \
                    found_schema_description != schema_description:
                raise MetaBException(
                    MetaBException.NOT_MATCHING,
                    "Schema name or description does not match defined values.")
