import re

pattern = '[A-Z]{4}\d{4}$'

def normalize(value):
    return re.sub('[^0-9A-Z]', '', value.upper())
