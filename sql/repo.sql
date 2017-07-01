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

-- @brief LSST Database Schema for Metadata Store, table containing
-- information about repositories.
--
-- @author Jacek Becla, SLAC


CREATE TABLE Repo
    -- <descr>Information about repositories, one row per repo.
    -- A repository can be a database, a directory with files.
    -- This is a global table, (there is only one in the entire Metadata Store).
    -- </descr>
(
    repoId INT NOT NULL AUTO_INCREMENT,
    url VARCHAR(255),
        -- <descr>Virtual location.</descr>
    projectId INT,
        -- <descr>Id of the project this data set comes from. References an entry
        -- in the Project table.</descr>
    repoType ENUM('db', 'butler', 'file', 'custom'),
    lsstLevel ENUM ('DC', 'L1', 'L2', 'L3', 'dev'),
        -- <descr>Supported levels: DC ('Data Challenge'), L1 ('Level 1 / Alert
        -- Production'), L2 ('Level 2 / Data Release'), L3 ('Level 3 / User data'),
        -- dev ('unclassified development')</descr>
    dataRelease VARCHAR(64),
        -- <descr>Data Release, if applicable.</descr>
    version VARCHAR(255),
    shortName VARCHAR(255),
    description VARCHAR(255),
    ownerId INT,
        -- <descr>References entry in User table</descr>
    checksum VARCHAR(128),
    createTime DATETIME,
    ingestTime DATETIME,
    priorityLevel ENUM('scratch', 'keepShort', 'keepLong'),
        -- <descr>This will be useful when purging.</descr>
    availability ENUM('loading', 'published', 'notPublished', 'locked4Maintenance'),
        -- <descr>I am sure there are many more states we can come up
        -- with.</descr>
    accessibility ENUM('public', 'private', 'unreleased'),
        -- <descr>If we want to do in that direction, we'd need to cover
        -- group access too.</descr>
    onDisk BOOL,
        -- <descr>Bitwise OR describing locations
        -- <ul>
        --    <li>0x1: in memory only
        --    <li>0x2: on ssd
        --    <li>0x4: on spinning disk
        --    <li>0x8: on tape
        -- </ul></descr>
    backupStatus ENUM('requested', 'ongoing', 'notBackedUp'),
    lastBackup DATETIME,
        -- <descr>Time of the completion of the last successful backup.
        -- </descr>
    PRIMARY KEY repo_repoId(repoId)
) ENGINE = InnoDB;
