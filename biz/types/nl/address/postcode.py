import re

pattern = '[1-9]\d{3}[A-Z]{2}$'

def normalize(value):
    return re.sub('[^0-9A-Z]', '', value.upper())
