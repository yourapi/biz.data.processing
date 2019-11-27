import re, sys, mongoengine
from indroid.object.customer import Address, Email, Company, Person
from indroid.object.general import Qualifier
from client.kpn.object.product.swol import ProductSwol, ProductSwolInstance, DomainSwolInstance
#from bizold.input.file import StringReader, StringWriter
from indroid.input.csv_reader import UnicodeWriter, UnicodeDictWriter
from datetime import datetime
import indroid.input.csv as csv
import indroid.input.xml as xml

def get_classes(cls=Company):
    def get_classes_worker(cls, result):
        if cls not in result:
            result.append(cls)
        for c in cls.__subclasses__():
            get_classes_worker(c, result)
    result = []
    get_classes_worker(cls, result)
    for c in result:
        print c.__name__, c

def test_odin(file_in=r"P:\data\source\kpn\swol_marketing\odin\2012-11-14\extract\odin_dump.csv",
              every=1, offset=0):
    exceptions = set()
    f_in = csv.Source(file_in).table(0)
    for i, row in enumerate(f_in):
        if i % every != offset:
            continue
        try:
            # Make necessary objects for all fields and store them in the DB
            product = ProductSwol.new(row['package'])
            # ProductInstance als factory van product nemen.
            domain = DomainSwolInstance.new(domain=row['data_domain'])  # Hm als apart object aanmaken of als onderdeel van Product laten aanmaken?
            product_instance = product.factory(id_odin=row['odin_id'], domain=domain, status=row['status'], date_purchase=row['date_created'])
            address = Address.new(zipcode=row['postcode'], number=row['huisnr'], extension=row['huisnr'], street=row['address'], city=row['city'])
            email = Email.new(address=row['email'])
            product_instance.save()
            company = Company(name=row['org_name'])
            person = Person(name=row['lastname'], initials=row['initials'], infix=row['infix'])
            company.products.append(product_instance)
            for item in company, person:
                item.address.append(address)
                item.email.append(email)
            company.save()
            person.link(company)  # person is saved as a side-effect
        except Exception as e:
            if e.message not in exceptions:
                print i, e
                exceptions.add(e.message)
        if i % 10000 == 0:
            print i, datetime.now()
        #if i > 1000:
            #break
    ProductSwol.relate()

def test_search():
    for cls in (Address, Email, Company, Person, ProductSwol, ProductSwolInstance):
        print cls.__name__
        filename = r"P:\data\source\kpn\swol_marketing\odin\2012-11-14\extract\export\{}.csv".format(cls.__name__)
        f_out = UnicodeDictWriter(filename, fieldnames=cls.fieldnames())
        for obj in cls.objects():
            f_out.writerow(obj.row())
        f_out.stream.close()
    for i, c in enumerate(Company.objects(name__istartswith='ict').order_by('name')):
        print i, c.name, c.address
    for i, p in enumerate(Person.objects(name__istartswith='lutt')):
        print i, p.name, p.email, p.address

if __name__ == '__main__':
    #get_classes(list(Company._DocumentMetaclass__get_bases([Company]))[-1])
    c = mongoengine.connect('kpn_zm1')
    #c.drop_database('kpn_zm1')
    #test_odin(every=int(sys.argv[1]), offset=int(sys.argv[2]))
    test_search()
    