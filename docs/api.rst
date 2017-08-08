###################
API - `/meta/`
###################

The methods listed on this page allow you to list maintain builds (such as showing or deprecating them).

Methods
=======

- :http:get:`/meta/v1/db/` --- Show list of databases.
- :http:get:`/meta/v1/db/(string:db_id)/` --- Show database information.


Reference
=========

.. autoflask:: lsst.dax.metaserv.app:app
   :undoc-blueprints: metaREST
   :undoc-static:
   :order: path
   :endpoints: 
