"""Communication events between objects: company calls person, person mails to company etc."""
from ming import schema
from ming.odm import FieldProperty, ForeignIdProperty, RelationProperty
from ming.odm import Mapper
from ming.odm.declarative import MappedClass
from db import session
import indroid.nl.schema as nls
import internet.schema as ins

class Event(MappedClass):
    class __mongometa__:
        session = session
        name = 'Event'
    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(str)

class Email(Event):
    class __mongometa__:
        session = session
        name = 'Email'
    _id = FieldProperty(ins.Email)

class Call(Channel):
    class __mongometa__:
        session = session
        name = 'Call'

class RadioAd(Channel):
    class __mongometa__:
        session = session
        name = 'RadioAd'

Mapper.compile_all()
