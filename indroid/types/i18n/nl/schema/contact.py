import re

class ZipCode(StringField):
    ZIPCODE_REGEX = re.compile('[1-9]\d{3}[A-Z]{2}')
    def validate(self, value):
        "The Postcode-value is cleansed, after cleansing, the value is validated against the valid pattern."
        value = self.to_mongo(value)
        super(ZipCode, self).validate(value)
        if not self.ZIPCODE_REGEX.match(value):
            self.error('Invalid zipcode: {}'.format(value))
    def to_mongo(self, value):
        if not isinstance(value, basestring):
            return value
        # Remove all non-alphanumerics:
        return re.sub('[^A-Z0-9]', '', value.upper())

class Phonenumber(StringField):
    "The phonenumber is cleansed, after cleansing the value is validated."
    COUNTRY = 'NL'
    PREFIX = '+31'
    PREFIXES_ALTERNATIVE = ['0031', '31', '0']  # Must be list to enable regex-matching
    AREACODES = set("""010 013 015 020 023 024 026 030 033 035 036 038 040 043 045 046 050 053 055 
    058 070 071 072 073 074 075 076 077 078 079 
    0111 0113 0114 0115 0117 0118 0161 0162 0164 0165 0166 0167 0168 0172 0174 0180 0181 0182 0183 
    0184 0186 0187 0222 0223 0224 0226 0227 0228 0229 0251 0252 0255 0294 0297 0299 0313 0314 0315 
    0316 0317 0318 0320 0321 0341 0342 0343 0344 0345 0346 0347 0348 0411 0412 0413 0416 0418 0475 
    0478 0481 0485 0486 0487 0488 0492 0493 0495 0497 0499 0511 0512 0513 0514 0515 0516 0517 0518 
    0519 0521 0522 0523 0524 0525 0527 0528 0529 0541 0543 0544 0545 0546 0547 0548 0561 0562 0566 
    0570 0571 0572 0573 0575 0577 0578 0591 0592 0593 0594 0595 0596 0597 0598 0599 06 0800 0900 085 088""".split())
    AREACODES_LEN = min([len(a) for a in AREACODES]), max(len(a) for a in AREACODES) + 1
    def __init__(self, **kwargs):
        super(Phonenumber, self).__init__(regex='({})\d{9}'.format('|'.join(self.PREFIXES_ALTERNATIVE + [self.PREFIX])), max_length=13, min_length=9, **kwargs)
    def validate(self, value):
        "The value is cleansed, after cleansing, the value is validated against the valid pattern."
        value = self.to_mongo(value)
        super(Phonenumber, self).validate(value)
        found = False
        for length in range(*self.AREACODES_LEN):
            if value[:length] in self.AREACODES:
                found = True
                break
        if not found:
            self.error('Unknown area code in {}'.format(value))
    def to_mongo(self, value):
        if not isinstance(value, basestring):
            return value
        # Remove all non-alphanumerics:
        return re.sub('[^0-9\+]', '', value.upper())

if __name__ == '__main__':
    p = Phonenumber()
    z = ZipCode()
    print p.validate('0594507853')
    print p.validate('(--31)594-50 78 53')
    print p.validate('Geheim telefoonnummer: 040-31 14 111 (niet bellen)')
    print z.validate('1000AA')
    print z.validate('1234 bc!!')
    print z.validate('!2@3#4$5%:c...z!')
    print z.validate_try('1000CC')
    print p.validate_try('Geheim telefoonnummer: 0111-31 14 11 (twee x bellen!)')