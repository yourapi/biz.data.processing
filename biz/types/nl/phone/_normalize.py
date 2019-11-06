import re
def normalize(value):
    "Return a valid phonenumber with general separators removed. Keep letters to prevent false positives."
    return re.sub('[^0-9\+a-zA-Z]', '', value)
