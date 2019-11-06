from mongoengine import Document
from common import Singleton, BizBase
import biz.schema.internet as ins

class IP(Document, Singleton, BizBase):
    meta = {'allow_inheritance': True}
    ip = ins.IPField(primary_key=True)
    def __str__(self):
        return ins.IPField.to_string(self.ip)

class Domain(Document, Singleton, BizBase):
    meta = {'allow_inheritance': True}
    domain = ins.DomainField(primary_key=True)
    def tld(self):
        return self.domain.split('.')[-1]
    def __str__(self):
        return self.domain
