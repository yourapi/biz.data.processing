import postcode, housenumber
pattern = postcode.pattern[:-1] + housenumber.pattern
postcode_len = 6

def normalize(value):
    "Clean up the postcode 2013-11-20"
    l = postcode_len
    while l < len(value) and len(postcode.normalize(value[:l])) < postcode_len:
        l += 1
    return postcode.normalize(value[:l]) + value[l:]