import re, datetime
from dateutil import parser

replaces = 'jan:jan,feb:feb,maa:mar,apr:apr,mei:may,jun:jun,jul:jul,aug:aug,sep:sep,okt:oct,nov:nov,dec:dec,'\
'januari:january,februari:february,maart:march,april:april,mei:may,juni:june,juli:july,augustus:august,'\
'september:september,oktober:october,november:november,december:december,'\
'maa:mon,din:tue,woe:wed,don:thu,vri:fri,zat:sat,zon:sun,'\
'maandag:monday,dinsdag:tuesday,woensdag:wednesday,donderdag:thursday,vrijdag:friday,zaterdag:saturday,zondag:sunday,'\
'uren:hours,minuten:minutes,secondes:seconds,seconden:seconds,uur:hour,minuut:minute,seconde:second,u:h,m:m,s:s'
replaces = dict([pair.split(':') for pair in replaces.split(',')])

def parse(timestr, parserinfo=None, **kwargs):
    "Return a localized version of the date/time-parser from dateutil.parser. Current parser is English only."
    if isinstance(timestr, (datetime.date, datetime.datetime)):
        return timestr
    timestr = ' '.join([replaces[part.lower()] if part.lower() in replaces else part for part in timestr.split()])
    return parser.parse(timestr, parserinfo, **kwargs)

if __name__ == '__main__':
    print parse('woensdag 13 oktober 1994 at 14:00')