#!/usr/bin/env python

from lsst.db.engineFactory import getEngineFromFile
from lsst.db import utils

queries = (
'''INSERT INTO User(userId, mysqlUserName, firstName, lastName, affiliation) VALUES
  (1, 'prod',  'production', '', 'LSST'),
  (2, 'jb',    'jacek',      'brown', 'SLAC'),
  (3, 'johnS', 'john',       'smith', 'IPAC')''',

'''INSERT INTO Repo(repoId, url, project, repoType, lsstLevel, shortName, owner) VALUES
  (1, '/u1/mysql_data/AlertProd',          'LSST', 'db',   'L1', 'current AlertProd',     1),
  (2, '/u1/mysql_data/DR1'         ,       'LSST', 'db',   'L2', 'DR1',                   1),
  (3, '/u1/mysql_data/W13_stripe82',       'LSST', 'db',   'DC', 'official W13_stripe82', 1),
  (4, '/u1/mysql_data/S13_stripe82',       'LSST', 'db',   'DC', 'release candidate S13', 1),
  (5, '/u1/mysql_data/jacek_db1x',         'LSST', 'db',   'L3', 'jacek_db1x',            2),
  (6, '/u1/mysql_data/john_tmpDb5',        'LSST', 'db',   'L3', 'john_tmpDb5',           3),
  (7, '/u1/images/external/SDSS/Stripe82', 'SDSS', 'file', 'L2', 'SDSS_stripe82',         1),
  (8, '/u1/images/lsst/raw',               'LSST', 'file', 'L1', 'raw',                   1),
  (9, '/u1/images/lsst/coadds',            'LSST', 'file', 'L2', 'coadds',                1)''',

'''INSERT INTO DbMeta(dbMetaId, dbName) VALUES
  (1, 'AlertProd'),
  (2, 'DR1'),
  (3, 'W13'),
  (4, 'S13_v0.7'),
  (5, 'jacek_db1x'),
  (6, 'john_tmpDb5')''',

'''INSERT INTO DDT_Table(tableId, dbMetaId, tableName, descr) VALUES
  (1, 5, 'Object',     'my object tablexx'),
  (2, 5, 'Source',     'source table'),
  (3, 6, 'DeepSource', 'deep src')''',

'''INSERT INTO DDT_Column(columnId, columnName, tableId, descr, ucd, units) VALUES
  ( 1, 'oId',    1, 'the object id, PK', 'meta.id;src', ''),
  ( 2, 'PS_ra',  1, 'right ascension',   'pos.eq.ra',   'degree'),
  ( 3, 'PS_decl',1, 'declination',       'pos.eq.dec',  'degree'),
  ( 4, 'flux',   1, 'measured flux',     'phot.count',  'nmgy'),
  ( 5, 'sId',    2, 'source id, PK',     'meta.id;src', ''),
  ( 6, 'oId',    2, 'ptr to object',     'meta.id;src', ''),
  ( 7, 'ra',     2, 'right ascension',   'pos.eq.ra',   'degree'),
  ( 8, 'decl',   2, 'declination',       'pos.eq.dec',  'degree'),
  ( 9, 'flux',   2, 'measured flux',     'phot.count',  'nmgy'),
  (10, 'dsId',   3, 'deep src id, PK',   'meta.id;src', ''),
  (11, 'flags',  3, 'my flags',          '',            '')'''
)

conn = getEngineFromFile("~/.lsst/dbAuth-metaServ.txt").connect()

utils.dropDb(conn, "metaServ_core", mustExist=False)
utils.createDb(conn, "metaServ_core")
utils.loadSqlScript(conn, "sql/global.sql")
utils.loadSqlScript(conn, "sql/dbRepo.sql")
utils.loadSqlScript(conn, "sql/fileRepo.sql")
utils.useDb(conn, "metaServ_core")
for q in queries:
    conn.execute(q)
