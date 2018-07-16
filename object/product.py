from mongoengine import Document, DynamicDocument, StringField, IntField, ReferenceField,\
     ListField, GenericReferenceField, DateTimeField
from common import BizBase
from biz.schema.general import BizDateTimeField

class Product(DynamicDocument, BizBase):
    """Base of all specific Product-classes for each industry and company.
    A Product actually describes a product-class, NOT a product-instance. E.g. consider the following:
    Product
    |-Car
      |-ElectricCar
    then a list of Product-instances may look like:
    (1) brand: Toyota  model: Prius Plugin type: Executive
    (2) brand: Toyota  model: Prius Plugin type: Executive
    (3) brand: Toyota  model: Prius Plugin type: Comfort
    (4) brand: Toyota  model: Prius Plugin type: Business
    A specific instance is created using ProductInstance, with additional data like color, 
    mileage etc."""
    name = StringField()
    type_of = GenericReferenceField()
    part_of = ListField(GenericReferenceField())  # Originaly it was a ReferenceField('self'), but this can break if a class
    parts = ListField(GenericReferenceField())    # inherits from Product and 
    variety = StringField()
    meta = {'allow_inheritance': True,
            'indexes': ['name', ('name', 'variety')]}
    def factory(self, **kwargs):
        """abstract factory for instances. Tbd: how instances are created and connected to their product.
        Work out a complete example for Odin to clear things up."""
        kwargs['product'] = self
        return ProductInstance(**kwargs)

class ProductInstance(DynamicDocument, BizBase):
    """Specific instance of a Product. To be combined with all relevant data about
    a product instance, like channel, discount, date of purchase etc."""
    meta = {'allow_inheritance': True}
    product = ReferenceField(Product)
    amount = IntField(default=1)
    date_purchase = BizDateTimeField()
    date_start = BizDateTimeField()
    date_end = BizDateTimeField()