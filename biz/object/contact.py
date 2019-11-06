from mongoengine import Document, StringField, IntField
from common import Singleton, BizBase
from biz.schema.general import BizStringField, BizIntField
from biz.i18n.nl.schema.contact import ZipCode
from biz.schema.contact import HousenumberField, ExtensionField
from biz.schema.internet import EmailField

class Email(Document, Singleton, BizBase):
    address = EmailField(primary_key=True)
    def accountname(self):
        return self.address.split('@')[0]
    def domain(self):
        return self.address.split('@')[-1]
    def __str__(self):
        return self.address

class Address(Document, Singleton, BizBase):
    # ToDo: connect with standard zip-codes, do a street lookup and cleanse the address.
    street = StringField()
    number = HousenumberField(default=-1)
    extension = ExtensionField()
    zipcode = ZipCode(unique_with=('number', 'extension'))
    city = StringField()
    __str__ = Singleton.__str__

