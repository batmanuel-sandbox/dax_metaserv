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
import click
import os

from sqlalchemy.orm import sessionmaker
from lsst.db.engineFactory import getEngineFromFile
from .schemaToMeta import parse_schema
from .metaBException import MetaBException
from .model import MSUser, MSRepo, MSDatabase, MSDatabaseSchema, MSDatabaseTable, MSDatabaseColumn


class CliConfig(object):
    def __init__(self, config_path):
        self.engine = getEngineFromFile(config_path)


pass_config = click.make_pass_decorator(CliConfig)


@click.group()
@click.option('config', '--config', envvar='MS_CONFIG',
              default=os.path.expanduser("~/.lsst/metaserv.ini"),
              help='Config file location',
              type=click.Path())
@click.option('--verbose', '-v', is_flag=True,
              help='Enables verbose mode.')
@click.pass_context
def cli(ctx, config, verbose):
    ctx.obj = CliConfig(config)
    ctx.obj.verbose = verbose
    ctx.obj.Session = sessionmaker(ctx.obj.engine)
    ctx.obj.log = log.getLogger("lsst.metaserv.admin")


@cli.command("init-db")
@pass_config
def init_db(config):
    """Initialize Database"""
    return _init_db(config)


def _init_db(config):
    from .model import Base
    Base.metadata.create_all(config.engine, checkfirst=True)


@cli.command("reinit-db")
@pass_config
def reinit_db(config):
    """Drops the database and reinitializes it."""
    from .model import Base
    Base.metadata.drop_all(config.engine)
    _init_db(config)


@cli.command("add-db")
@click.argument("schema_file")
@click.argument("db_name")
@click.argument("host")
@click.argument("port")
@click.argument("schema_name")
@click.argument("schema_description")
@click.argument("owner")
@click.argument("lsst_level", required=False)
@click.argument("data_release", required=False)
@click.argument("target_engine", required=False)
@pass_config
def add_db(config, schema_file, db_name, host, port, schema_name,
           schema_version, schema_description, lsst_level, data_release,
           owner, target_engine=None):
    """Add a database.

    :param schema_file: ascii file containing schema with
    description.

    :param db_name: database name

    :param host: Hostname of this database

    :param port: Port the database is reachable on.

    :param schema_name: name of default schema associated with
    the schema file.

    :param schema_description: Description of the default schema

    :param schema_version: Description of the default schema

    :param owner: owner of the database

    :param lsst_level: level (e.g., L1, L2, L3)

    :param data_release: Associated Data Release

    :param target_engine: If provided , this engine will be used to
    check that metadata will be consistent with what's loaded
    in the target_engine's database.

    """

    # Parse the ascii schema file
    parsed_schema = parse_schema(schema_file)

    if target_engine:
        _check_schema_consistency(config,
            db_name, schema_name, parsed_schema, schema_version,
            schema_description, target_engine)

    def add_repo(session, db_name, schema_description,
                 user, lsst_level, data_release):
        repo = session.query(MSRepo).filter(MSRepo.name == db_name).scalar()
        if repo:
            raise MetaBException(MetaBException.NOT_MATCHING, "Repo exists")

        # Everything else is guaranteed not to exist
        # FIXME: Repo Name is the same as Database Name, for now
        repo = MSRepo(name=db_name,
                      description=schema_description,
                      user_id=user.user_id,
                      lsst_level=lsst_level,
                      data_release=data_release)
        session.add(repo)
        session.flush()
        return repo

    def add_database(session, repo, db_name, conn_host, conn_port):
        db = MSDatabase(repo_id=repo.repo_id, name=db_name,
                        conn_host=conn_host, conn_port=conn_port)
        session.add(db)
        session.flush()
        return db

    def add_schema(session, db, schema_name, is_default_schema=True):
        schema = MSDatabaseSchema(db_id=db.db_id, name=schema_name,
                                  is_default_schema=is_default_schema)
        session.add(schema)
        session.flush()
        return schema

    def add_tables_and_columns(session, schema, parsed_schema):
        for table_name in parsed_schema:
            table_data = parsed_schema[table_name]

            table = MSDatabaseTable(
                name=table_name,
                schema_id=schema.schema_id,
                description=table_data.get("description", "")
            )
            session.add(table)
            session.flush()
            columns = table_data["columns"]
            for col, ord_pos in zip(columns, range(len(columns))):
                print(col)
                column = MSDatabaseColumn(
                    table_id=table.table_id,
                    name=col["name"],
                    description=col.get("description", ""),
                    ordinal=ord_pos,
                    ucd=col.get("ucd", ""),
                    unit=col.get("unit", "")
                )
                session.add(column)
            session.flush()

    # Now, we will be talking to the metaserv database, so change
    # connection as needed
    session = config.Session()

    user = session.query(MSUser).filter(MSUser.email == owner).scalar()

    if not user:
        config.log.error("Owner '%s' not found.", owner)
        raise MetaBException(MetaBException.OWNER_NOT_FOUND, owner)
    try:
        repo = add_repo(session, db_name, schema_description, user, lsst_level,
                        data_release)

        db = add_database(session, repo, db_name, host, port)
        schema = add_schema(session, db, schema_name)
        add_tables_and_columns(session, schema, parsed_schema)
        session.commit()
    except Exception as e:
        print(dir(e))
        session.rollback()
        raise e
    finally:
        session.close()
    return db


@cli.command("add-user")
@click.argument("first_name")
@click.argument("last_name")
@click.argument("email")
@pass_config
def add_user(config, email, first_name, last_name):
    """Add user."""

    session = config.Session()
    try:
        user = MSUser(first_name=first_name, last_name=last_name, email=email)
        session.add(user)
        session.commit()
        return user
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def _check_schema_consistency(config, db_name, schema_name, parsed_schema,
                              schema_version, schema_description,
                              target_engine):
    # Connect to the server that has database that is being added
    from sqlalchemy.engine.reflection import Inspector

    inspector = Inspector(target_engine)

    if schema_name not in inspector.get_schema_names():
        config.log.error("Schema '%s' not found.", db_name)
        raise MetaBException(MetaBException.DB_DOES_NOT_EXIST, db_name)

    db_tables = inspector.get_table_names(schema=schema_name)

    for table_name, parsed_table in parsed_schema.items():
        # Check parsed tables - we allow other tables in schema
        if table_name not in db_tables:
            config.log.error(
                "Table '%s' not found in db, present in ascii file.",
                table_name)
            raise MetaBException(MetaBException.TB_NOT_IN_DB, table_name)

        db_columns = inspector.get_columns(table_name=table_name,
                                           schema=schema_name)
        parsed_columns = parsed_table["columns"]
        if len(parsed_columns) != len(db_columns):
            config.log.error("Number of columns in db for table %s (%d) "
                             "differs from number columns in schema (%d)",
                             table_name, len(db_columns),
                             len(parsed_columns))
            raise MetaBException(MetaBException.NOT_MATCHING)

        for column in parsed_columns:
            column_name = column["name"]
            if column_name not in db_columns:
                config.log.error(
                    "Column '%s.%s' not found in db, "
                    "but exists in schema DDL",
                    table_name, column_name)
                raise MetaBException(MetaBException.COL_NOT_IN_TB,
                                     column_name, table_name)

    # Get schema description and version, it is ok if it is missing
    ret = target_engine.execute(
        "SELECT version, descr FROM %s.ZZZ_Schema_Description" % db_name)
    if ret.rowcount != 1:
        config.log.error(
            "Db '%s' does not contain schema version/description", db_name)
    else:
        (found_schema_version, found_schema_description) = ret.first()
        if found_schema_version != schema_version or \
                found_schema_description != schema_description:
            raise MetaBException(
                MetaBException.NOT_MATCHING,
                "Schema name or description does not match defined values.")

if __name__ == '__main__':
    cli()
