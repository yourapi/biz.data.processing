from ming import schema
from ming.odm import FieldProperty, ForeignIdProperty, RelationProperty
from ming.odm import Mapper
from ming.odm.declarative import MappedClass
from db import session
import biz.nl.schema as nls
import internet.schema as ins

class Payment(MappedClass):
    class __mongometa__:
        session = session
        name = 'Payment'
    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(str)

Mapper.compile_all()
