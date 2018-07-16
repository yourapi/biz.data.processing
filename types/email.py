import re
import tld

pattern = (r"(^[-!#$%&'*+/=?^_`{}|~0-9a-z]+(\.[-!#$%&'*+/=?^_`{}|~0-9a-z]+)*"  # dot-atom
    r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-011\013\014\016-\177])*"' # quoted-string
    r')@(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,6}\.?$')  # domainpattern_flags = re.I

def normalize(value):
    return value.lower().strip()

def validate(value):
    "Pattern must match and tld must be valid. 2013-11-01"
    ext = value.split('.')[-1]
    return re.match(pattern, value, re.I) and tld.validate(ext)

if __name__ == '__main__':
    #for em in 'tESt@tesT.nl, test@TEsT.eu ,\t wrong@domain.tld \t,1test@www.www, wrong all together a@b.cc'.split(','):
    for em in '1@1.nl,erbo-bouw/adviesburo@hetnet.com,gijsvandenbroek#@tvk.nl,koos.leeuwenstein!@cofarno.nl,shokuhnajfi!@yahoo.com'.split(','):
        print em, normalize(em), validate(normalize(em))