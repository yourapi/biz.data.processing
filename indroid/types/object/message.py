"""Message which is exchanged: contents of a call, contents of an ad."""
from ming import schema
from ming.odm import FieldProperty, ForeignIdProperty, RelationProperty
from ming.odm import Mapper
from ming.odm.declarative import MappedClass
from db import session
import indroid.nl.schema as nls
import internet.schema as ins

class Message(MappedClass):
    class __mongometa__:
        session = session
        name = 'Message'
    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(str)

class Email(Message):
    class __mongometa__:
        session = session
        name = 'Email'
    _id = FieldProperty(ins.Email)

class Call(Message):
    class __mongometa__:
        session = session
        name = 'Call'

class Ad(Message):
    class __mongometa__:
        session = session
        name = 'Ad'
    

Mapper.compile_all()
