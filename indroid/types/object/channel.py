"""A channel is a means of contact between company and customer: send an email, visit website, phonecall"""
from ming import schema
from ming.odm import FieldProperty, ForeignIdProperty, RelationProperty
from ming.odm import Mapper
from ming.odm.declarative import MappedClass
from db import session
import indroid.nl.schema as nls
import internet.schema as ins

class Channel(MappedClass):
    class __mongometa__:
        session = session
        name = 'Channel'
    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(str)

class Email(Channel):
    class __mongometa__:
        session = session
        name = 'Email'
    _id = FieldProperty(ins.Email)

class Outbound(Channel):
    class __mongometa__:
        session = session
        name = 'Outbound'

class Inbound(Channel):
    class __mongometa__:
        session = session
        name = 'Inbound'

Mapper.compile_all()
