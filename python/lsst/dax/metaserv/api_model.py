from marshmallow import Schema, fields
from flask import request, url_for


def db_url(db):
    db_id = request.view_args.get("db_id", db.id)
    schema_id = request.view_args.get("schema_id", None)
    return url_for(".database", schema_id=schema_id, db_id=db_id,
                   _external=True)


def schema_url(schema):
    db_id = request.database.id
    schema_id = schema.id
    return url_for(".tables", schema_id=schema_id, db_id=db_id,
                   _external=True)


def table_url(table):
    db_id = request.database.id
    schema_id = request.view_args.get("schema_id", None)
    return url_for(".table", schema_id=schema_id, db_id=db_id,
                   table_id=table.id, _external=True)


class Database(Schema):
    class Meta:
        ordered = True

    name = fields.String()
    id = fields.Integer()
    url = fields.Function(db_url)
    host = fields.String(attribute="conn_host")
    port = fields.Integer(attribute="conn_port")
    default_schema = fields.Function(
        lambda obj: obj.default_schema.first().name)


class DatabaseSchema(Schema):
    class Meta:
        ordered = True

    name = fields.String()
    id = fields.Integer()
    url = fields.Function(schema_url)
    description = fields.String()
    is_default_schema = fields.Boolean()


class DatabaseColumn(Schema):
    class Meta:
        ordered = True

    name = fields.String()
    id = fields.Integer()
    description = fields.String()
    ordinal = fields.Integer()
    # May need to be many:one relationship
    ucd = fields.String()
    unit = fields.String()
    datatype = fields.String()
    nullable = fields.Boolean()
    arraysize = fields.Integer()


class DatabaseTable(Schema):
    class Meta:
        ordered = True

    name = fields.String()
    id = fields.Integer()
    url = fields.Function(table_url)
    description = fields.String()
    columns = fields.Nested(DatabaseColumn, many=True)


# if __name__ == '__main__':
#     class Mock(object):
#         pass
#
#     obj = Mock()
#     default_schema = Mock()
#     default_schema.name = "my_example"
#     data = {"name": "test",
#             "conn_host": "example.com",
#             "conn_port": 80,
#             "default_schema": default_schema
#             }
#     obj.__dict__.update(data)
#     db_schema = Database()
#     print(db_schema.dumps(obj).data)
#
#     db_schema_basic = Database(only=("name", "host", "port"))
#     print(db_schema_basic.dumps(obj).data)
