import sys, re, datetime, neo4j
from bulbs.model import Node, Relationship
from bulbs.property import String, Integer, DateTime
from bulbs.utils import get_logger
from bulbs.utils import current_datetime
from bulbs.neo4jserver import Graph
from bulbs.neo4jserver.batch import Neo4jBatchClient
from bizold.input.file import StringReader, StringWriter
log = get_logger(__name__)
graph = Graph()

class BizBase(object):
    @classmethod
    def new(cls, graph=graph,**kwargs):
        proxy = getattr(graph, cls.element_type)
        return proxy.create(**kwargs)
    def __repr__(self):
        result = []
        for prop in self.__class__._get_initial_properties().values():
            result.append([prop.name, getattr(self, prop.name)])
        return '<{}-{}: {}>'.format(self.__class__.__name__, self.eid, ', '.join(['{}: {}'.format(*t) for t in result]))
    __str__ = __repr__

class Singleton(BizBase):
    """Class for umltiple inheritance. Together with a neo4j-Node ensures unique document:
    if a node with these keys already exists, return it. If not, create with the supplied parameters.
    If an existing node with different parameters is supplied, the different values are ignored.
    class NewDocument(BaseDocument,Singleton):
        """
    # ToDo: see if metaclass can help cache the class-properties (now ' new'  inspects properties every time)
    @classmethod
    def new(cls, graph=graph, **kwargs):
        search_args = {}
        for prop in cls._get_initial_properties().values():
            if prop.unique and prop.name in kwargs:
                search_args[prop.name] = kwargs[prop.name]
        proxy = getattr(graph, cls.element_type)
        try:
            nodes = proxy.index.lookup(**search_args)
            if nodes:
                return nodes.next()
            else:
                return proxy.create(**kwargs)
        except LookupError:
            return proxy.create(**kwargs)

class Customer(BizBase, Node):
    element_type = 'customer'
    name = String(indexed=True, unique=True)

class Company(Customer):
    """A company with 0 or more employees. In case of an implied company (person without formal company)
    the company and person are equal."""
    element_type = 'Company'

class Person(Customer):
    "The name of the person is the surname."
    element_type = 'Person' # ToDo: create in meta-class
    initials = String(indexed=True, unique=True)
    infix = String(indexed=True, unique=True)

class Product(BizBase, Node):
    "Product, categroy and product-instance. Will be detailed later on."
    element_type = 'Product'
    name = String(indexed=True)
    date_start = DateTime()
    date_activated = DateTime()
    date_end = DateTime()
    amount = Integer()

class DomainField(String):
    "Valid domain, incl checking of validity of TLD."
    def validate(self, key, value):
        super(DomainField, self).validate(key, value)
        if '.' not in value and not re.match('\w+\d+', value):
            log.error('Invalid domain: {}'.format(value))
            raise ValueError

class EmailField(String):
    "Valid e-mail-address. Validation TBD, borrow from mongoengine..."
    def validate(self, key, value):
        super(EmailField, self).validate(key, value)
        if '@' not in value:
            log.error('Invalid e-mail: {}'.format(value))
            raise ValueError
    def coerce(self, key, value):
        return value.strip().lower()

class Domain(Singleton, Node):
    element_type = 'Domain'
    name = DomainField(unique=True, indexed=True)

class Email(Singleton, Node):
    element_type = 'Email'
    address = EmailField(unique=True, indexed=True)

class City(Singleton, Node):
    element_type = 'city'
    name = String(unique=True, indexed=True)

class Zipcode(Singleton, Node):
    element_type = 'Zipcode'
    code = String(unique=True, indexed=True)

class Street(Singleton, Node):
    element_type = 'Street'
    name = String(unique=True, indexed=True)

class Address(BizBase, Node):
    element_type = 'Address'
    number = Integer()
    addition = String()
    @classmethod
    def new(cls, number, addition=None, city=None, zipcode=None, street=None, g=graph):
        """Create or retrieve new address. Number is mandatory, combination of city/street, 
        zipcode is mandatory, other params are optional."""
        c = City.new(name=city)
        z = Zipcode.new(code=zipcode)
        s = Street.new(name=street)
        a = g.Address.create(number=number, addition=addition)
        g.AddressCity.create(a, c)
        g.AddressZipcode.create(a, z)
        g.AddressStreet.create(a, s)
        return a

class AddressCity(Relationship):
    label = 'AddressCity'
class AddressZipcode(Relationship):
    label = 'AddressZipcode'
class AddressStreet(Relationship):
    label = 'AddressStreet'

# Relations must get a different name; these names also depict entities.
class Location(Relationship):
    label = 'Location'

class Contact(Relationship):
    "The relation betwee a customer (or descendant) and a contact means (email, address, phone etc)."
    label = "Contact"
    medium = String()

class Employee(Relationship):
    label = 'Employee'

class Has(Relationship):
    label = 'Has'

# ToDo: automatic registering of all ckases and relations!
node_types = (Company, Person, Email, Domain, City, Zipcode, Street, Address, Product, Customer)
relation_types = (Contact, AddressCity, AddressZipcode, AddressStreet, Employee, Has, Location)

for node_type in node_types:
    graph.add_proxy(node_type.element_type, node_type)
for relation_type in relation_types:
    graph.add_proxy(relation_type.label, relation_type)

def test_read(file_in=r"P:\data\source\kpn\swol_marketing\odin\2012-11-14\extract\odin_dump-small.csv",
              count=1000, every=10, offset=0):
    prev = datetime.datetime.now()
    for i, row in enumerate(StringReader(file_in)):
        if i < offset:
            continue
        if i % every == 0:
            print i, datetime.datetime.now(), (datetime.datetime.now() - prev).total_seconds() / every
            prev = datetime.datetime.now()
            for limit in (100, 1000):
                if i >= limit and every < limit:
                    every = limit
        try:
            for key in row:
                try:
                    row[key] = unicode(row[key])
                except:
                    row[key] = 'Geen unicode'
                if row[key].upper() == 'NULL':
                    row[key] = ''
            comp = Company.new(name=row['org_name'])
            email = Email.new(address=row['email'])
            person = Person.new(initials=row['initials'], infix=row['infix'], name=row['lastname'])
            dom = Domain.new(name=row['data_domain'])
            number, addition = re.sub('\D', '', row['huisnr']), re.sub('\d', '', row['huisnr']).strip()
            number = int(number) if number else None
            addition = addition.lower() if addition else None
            address = Address.new(number, addition, row['city'].upper(), row['postcode'].upper(), row['address'])
            product = Product.new(name=row['package'], date_start=row['date_created'])
            if comp and email:
                c = graph.Contact.create(comp, email)
                c.medium = email.element_type
            if person and email:
                c = graph.Contact.create(person, email)
                c.medium = email.element_type
            if person and comp:
                graph.Employee.create(person, comp)
            if comp and address:
                graph.Location.create(comp, address)
            if comp and product:
                graph.Has.create(comp, product)
            if product and dom:
                graph.Has.create(product, dom)
        except Exception as e:
            print i, e
        if i >= count + offset:
            break

def test_batch(file_in=r"P:\data\source\kpn\swol_marketing\odin\2012-11-14\extract\odin_dump-small.csv"):
    prev = datetime.datetime.now()
    cnt, total = 10, 100
    for i, row in enumerate(StringReader(file_in)):
        if i % cnt == 0:
            print i, datetime.datetime.now(), (datetime.datetime.now() - prev).total_seconds() / cnt
            prev = datetime.datetime.now()
            for limit in (100, 1000):
                if i >= limit:
                    cnt = limit
        try:
            for key in row:
                try:
                    row[key] = unicode(row[key])
                except:
                    row[key] = 'Geen unicode'
                if row[key].upper() == 'NULL':
                    row[key] = ''
            
        except Exception as e:
            print i, e
        if i >= total:
            break

def test_write(file_out=r"P:\data\source\kpn\swol_marketing\odin\2012-11-14\extract\export\dump.csv"):
    def lst(gen):
        if not gen:
            return ''
        return list(gen)
    f_out = StringWriter(file_out, fieldnames=['type', 'node', 'inV', 'outV', 'inE', 'outE'])
    for C in node_types:
        try:
            for node in getattr(graph, C.element_type).get_all():
                f_out.writerow({'type': C.element_type, 'node': node, 'inV': lst(node.inV()), 'outV': lst(node.outV()), 'inE': lst(node.inE()), 'outE': lst(node.outE())})
        except Exception as e:
            print e
    f_out.close()

if __name__ == '__main__':
    offset = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    if not offset:
        graph.clear()
    test_read(offset=offset)
    test_write()