import re, sys
from mongoengine import Document, StringField, GenericReferenceField, ReferenceField, ListField
from contact import Address, Email
from common import BizBase, Singleton
from general import Qualifier
from product import ProductInstance

class Customer(Document, BizBase):
    meta={'allow_inheritance': True}
    name = StringField()
    address = ListField(ReferenceField(Address))
    email = ListField(ReferenceField(Email))
    others = ListField(GenericReferenceField())
    products = ListField(ReferenceField(ProductInstance))
    def link(self, other, qualification=None):
        self.others.append(other)
        other.others.append(self)
        self.save()
        other.save()
        if qualification:
            return Qualifier.link(self, other, qualification)

class Company(Customer):
    """A company with 0 or more employees. In case of an implied company (person without formal company)
    the company and person are equal."""

class Person(Customer):
    "The name of the person is the surname."
    initials = StringField()
    firstname = StringField()
    infix = StringField()
