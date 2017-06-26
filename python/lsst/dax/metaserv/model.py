from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, Text, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class MSUser(Base):
    """Basic information about a registered user."""
    __tablename__ = 'MSUser'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    id = Column(Integer, primary_key=True)
    first_name = Column(String(64))
    last_name = Column(String(64))
    email = Column(String(64), unique=True)


class MSRepo(Base):
    """Information about repositories, one row per repo.
    A repository can be a database, a directory with files.
    This is a global table, (there is only one in the entire Metadata
    Store"""
    __tablename__ = 'MSRepo'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    id = Column(Integer, primary_key=True)
    #: The short name of this repository
    name = Column(String(128))
    #: Description of this repo
    description = Column(Text)
    user_id = Column(Integer, ForeignKey("MSUser.id"))
    create_time = Column(DateTime)
    #: Supported levels:
    #: DC ('Data Challenge'),
    #: L1 ('Level 1 / Alert Production'), L2 ('Level 2/ Data Release'),
    #: L3 ('Level 3 / User data'), dev ('unclassified development')
    lsst_level = Column(String(64))
    #: Data Release, if applicable.
    data_release = Column(String(64))


class MSDatabase(Base):
    __tablename__ = 'MSDatabase'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, ForeignKey("MSRepo.id"), nullable=True)
    name = Column(String(128))
    description = Column(Text)
    conn_host = Column(String(128))
    conn_port = Column(Integer)
    schemas = relationship("MSDatabaseSchema", lazy="dynamic")
    default_schema = relationship(
        "MSDatabaseSchema",
        primaryjoin="and_(MSDatabase.id == MSDatabaseSchema.db_id, "
                    "MSDatabaseSchema.is_default_schema == True)",
        lazy="dynamic")


class MSDatabaseSchema(Base):
    __tablename__ = 'MSDatabaseSchema'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    id = Column(Integer, primary_key=True)
    db_id = Column(Integer, ForeignKey("MSDatabase.id"))
    name = Column(String(128))
    description = Column(Text)
    is_default_schema = Column(Boolean)
    tables = relationship("MSDatabaseTable", lazy="dynamic")


class MSDatabaseTable(Base):
    __tablename__ = 'MSDatabaseTable'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    id = Column(Integer, primary_key=True)
    schema_id = Column(Integer, ForeignKey("MSDatabaseSchema.id"))
    name = Column(String(128))
    description = Column(Text)
    columns = relationship("MSDatabaseColumn", lazy="dynamic")

    def url(self, base):
        import os
        os.path.join()
        return "/".join(base)



class MSDatabaseColumn(Base):
    __tablename__ = 'MSDatabaseColumn'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey("MSDatabaseTable.id"))
    name = Column(String(128))
    description = Column(Text)
    ordinal = Column(Integer)
    # May need to be many:one relationship
    ucd = Column(String(1024))
    unit = Column(String(128))
    datatype = Column(String(32))
    nullable = Column(Boolean)
    arraysize = Column(Integer)


def init_db(engine):
    Base.metadata.create_all(engine, checkfirst=True)


def _reinit_db(engine):
    Base.metadata.drop_all(engine)
    init_db(engine)


def session_maker(engine):
    return sessionmaker(bind=engine)


"""
CREATE VIEW resource AS (
    SELECT 'ivo://lsst.org/' || name  "ivoid",
        'db' res_type,
        null res_title,
        null updated,
        description res_description,
        null creator_seq,
        null source_value,
        null waveband
    FROM MSDatabase
);
"""

"""
CREATE VIEW res_schema AS (
    SELECT db.ivoid || '/' || name  "ivoid",
        description as schema_description
    FROM MSDatabase db
    NATURAL JOIN MSDatabaseSchema schema_
);
"""

"""
CREATE VIEW res_table AS (
    SELECT schema_.ivoid || '/' || name  "ivoid"
    name table_name, description table_description, null table_utype
    FROM MSDatabaseSchema 
    FROM MSDatabaseTable
);
"""

"""
CREATE VIEW table_column AS (
    SELECT name table_name, description table_description, null table_utype
    FROM MSDatabaseColumn
);
"""
