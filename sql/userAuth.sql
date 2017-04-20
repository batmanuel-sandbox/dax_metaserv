-- LSST Data Management System
-- Copyright 2014-2015 AURA/LSST.
--
-- This product includes software developed by the
-- LSST Project (http://www.lsst.org/).
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the LSST License Statement and
-- the GNU General Public License along with this program.  If not,
-- see <https://www.lsstcorp.org/LegalNotices/>.

-- @brief LSST Database Schema for Metadata Store, tables containing
-- information about users, groups they belong to, and authorizations.
--
-- @author Jacek Becla, SLAC


CREATE TABLE User
    -- <descr>Basic information about every registered user. This is
    -- a global table, (there is only one in the entire Metadata Store).
    -- Credentials are handled separately. Ultimately this will be managed
    -- through LDAP.</descr>
(
    userId INT NOT NULL AUTO_INCREMENT,
        -- <descr>Unique identifier.</descr>
    firstName VARCHAR(64),
    lastName VARCHAR(64),
    email VARCHAR(64),
    PRIMARY KEY PK_User_userId(userId),
    UNIQUE UQ_User_email(email)
) ENGINE = InnoDB;

CREATE TABLE Project
    -- <descr>Projects, for which we have data sets tracked by metaserv</descr>
(
    projectId INT NOT NULL AUTO_INCREMENT,
        -- <descr>Unique identifier.</descr>
    projectName VARCHAR(64),
    PRIMARY KEY PK_Project_projectId(projectId),
    UNIQUE UQ_Project_projectName(projectName)
) ENGINE = InnoDB;
