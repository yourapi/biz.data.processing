pattern = '(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,6}\.?$'

def normalize(value):
    return value.strip().lower()
