import re, sys
from mongoengine import Document, StringField, ReferenceField, GenericReferenceField, EmbeddedDocumentField, ListField, DynamicEmbeddedDocument
from common import BizBase, Singleton

class Qualification(DynamicEmbeddedDocument):
    pass

# The qualifier-class is disconnected from the objects it connects.
class Qualifier(Document, BizBase):
    """Qualifies the relationship of the referenced objects.
    Links two objects together (attributes 'first' and 'second'). A simple qualification
    can be given in the string 'qualification', an elaborate one can be given in the
    dynamic embedded document 'doc'.
    When a qualification of three or more connected objects is needed, a new qualifier must 
    be designed."""
    first = GenericReferenceField()
    second = GenericReferenceField()
    qualification = StringField()
    doc = EmbeddedDocumentField(Qualification)
    @classmethod
    def link(cls, first, second, qualification=None):
        q = cls(first=first, second=second, qualification=qualification)
        q.save()
        return q
    @classmethod
    def get(cls, first, second, ignore_order=True):
        """Retrieve the qualifier for the specified objects. Try it both ways of ignore_order 
        is set (try first, second and then second, first). Return the simple qualification if 
        present; the doc overrules the qualification string if present."""
        q = cls.objects(first=first, second=second).first()
        if not q:
            q = cls.objects(first=second, second=first).first()
        if not q:
            return None
        return q.doc if q.doc else q.qualification
