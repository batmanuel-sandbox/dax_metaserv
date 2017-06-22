from marshmallow import Schema, fields


class Database(Schema):
    class Meta:
        ordered = True

    name = fields.String()
    host = fields.String(attribute="conn_host")
    port = fields.Integer(attribute="conn_port")
    default_schema = fields.Function(
        lambda obj: obj.default_schema.first().name)


class DatabaseSchema(Schema):
    class Meta:
        ordered = True

    name = fields.String()
    description = fields.String()
    is_default_schema = fields.Boolean()


class DatabaseColumn(Schema):
    class Meta:
        ordered = True

    name = fields.String()
    description = fields.String()
    ordinal = fields.Integer()
    # May need to be many:one relationship
    ucd = fields.String()
    unit = fields.String()
    datatype = fields.String()
    nullable = fields.Boolean()


class DatabaseTable(Schema):
    class Meta:
        ordered = True

    name = fields.String()
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
