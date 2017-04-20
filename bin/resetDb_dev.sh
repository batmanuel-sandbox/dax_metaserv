#!/bin/sh

# This is for development / testing environment.

# Note, to run it, replace:
#   XXX with your actual mysql user name
#   YYY with your actual musql password

# Of course, feel free to use host/port and credentials on your local copy of mysql

# Use with caution, as this will completely wipe out and recreate the database
# metaServ_core in the database server described in the ~/.lsst/dbAuth-metaServ.txt
mysql --defaults-file=~/.lsst/dbAuth-metaServ.txt -e "drop database XXX_metaServ_core"
mysql --defaults-file=~/.lsst/dbAuth-metaServ.txt --database="" -e "create database XXX_metaServ_core"
mysql --defaults-file=~/.lsst/dbAuth-metaServ.txt < sql/repo.sql
mysql --defaults-file=~/.lsst/dbAuth-metaServ.txt < sql/dbRepo.sql

# example ~/.lsst/dbAuth-metaServ.txt
# [mysql]
# host     = lsst10.ncsa.illinois.edu
# port     = 3306
# user     = XXX
# password = YYY
# database = XXX_metaServ_core
