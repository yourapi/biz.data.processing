import re

pattern = '([A-Z0-9]{4}\-?){5}'

def normalize(value):
    "Generate groups of 4 characters, separated by hyphen, if no hyphens present."
    # If no hyphens present, add them to groups of 4:
    if not '-' in value:
        value = '-'.join([value[i:i+4] for i in range(0, len(value), 4)])
    return re.sub('[^0-9A-Z\-]', '', value.upper())
