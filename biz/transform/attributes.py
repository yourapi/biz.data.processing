from ming import schema
import re

class Number(schema.Int):
    def _validate(self, value, **kw):
        "The Postcode-value is cleansed, after cleansing, the value is validated against the valid pattern."
        # Remove all non-alphanumerics:
        if isinstance(value, basestring):
            value = int(re.sub('\D', '', value))
        return super(Number, self)._validate(value)

class Geo(schema.Array):
    def __init__(self, required=schema.NoDefault, if_missing=schema.NoDefault):
        super(Geo, self).__init__(schema.Float, required=required, if_missing=if_missing)
    def _validate(self, value):
        super(Geo, self)._validate(value)
        if len(value) != 2:
            schema.Invalid('Geo expects pair of values, {} values found: "{}"'.format(len(value), repr(value)), value, None)
        return value
    