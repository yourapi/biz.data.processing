import re, datetime
from py2neo import neo4j
from bizold.input.file import StringReader, StringWriter
import logging
log = logging.getLogger(__name__)
I = neo4j.Direction.INCOMING
O = neo4j.Direction.OUTGOING
B = neo4j.Direction.BOTH

db = neo4j.GraphDatabaseService()
index_type = db.get_or_create_index(neo4j.Node, 'Types')
index_customers = db.get_or_create_index(neo4j.Node, 'customers')


def test_init():
    ref = db.get_node(0)
    if not ref.get_single_related_node(neo4j.Direction.OUTGOING, 'CUSTOMERS'):
        customers = db.create({'class': 'Customers'})[0]
        ref.create_relationship_to(customers, 'CUSTOMERS')

def test_read(file_in=r"P:\data\source\kpn\swol_marketing\odin\2012-11-14\extract\odin_dump-small.csv"):
    ref = db.get_node(0)
    customers = ref.get_related_nodes(O, 'CUSTOMERS')[0]
    prev = datetime.datetime.now()
    cnt, total = 10, 100
    for i, row in enumerate(StringReader(file_in)):
        if i % cnt == 0:
            print i, datetime.datetime.now(), (datetime.datetime.now() - prev).total_seconds() * 1000 / cnt
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
            c = db.create({'name': row['org_name']})[0]
            c.create_relationship_to()
            email = Email.new(address=row['email'])
            person = Person.new(initials=row['initials'], infix=row['infix'], name=row['lastname'])
            dom = Domain.new(name=row['data_domain'])
            number, addition = re.sub('\D', '', row['huisnr']), re.sub('\d', '', row['huisnr']).strip()
            number = int(number) if number else None
            addition = addition.lower() if addition else None
            address = Address.new(number, addition, row['city'].upper(), row['postcode'].upper(), row['address'])
            product = graph.product.create(name=row['package'], date_start=row['date_created'])
            if comp and email:
                c = graph.contact.create(comp, email)
                c.medium = email.element_type
            if person and email:
                c = graph.contact.create(person, email)
                c.medium = email.element_type
            if person and comp:
                graph.employee.create(person, comp)
            if comp and address:
                graph.location.create(comp, address)
            if comp and product:
                graph.has.create(comp, product)
            if product and dom:
                graph.has.create(product, dom)
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
    #db.clear()
    test_init()
    test_read()
    test_write()