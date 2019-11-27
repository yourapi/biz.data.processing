import re

pattern = '\w{2,13}'   # ATTENTION: ALWAYS close the RE with a $ for complete match!!!
# See http://data.iana.org/TLD/tlds-alpha-by-domain.txt for current list of tlds.
# Use _beautify for multi-line tlds and filtering of test-tlds like "xn--45brj9c"
# Version 2013110401, Last Updated Tue Nov  5 07:07:01 2013 UTC
tlds = set(
"ac academy actor ad ae aero af ag agency ai al am an ao aq ar arpa as asia at "
"au aw ax az ba bar bargains bb bd be berlin best bf bg bh bi bike biz bj blue "
"bm bn bo boutique br bs bt build builders buzz bv bw by bz ca cab camera camp "
"cards careers cat catering cc cd center ceo cf cg ch cheap christmas ci ck cl "
"cleaning clothing club cm cn co codes coffee com community company computer "
"condos construction contractors cool coop cr cruises cu cv cw cx cy cz dance "
"dating de democrat diamonds directory dj dk dm do domains dz ec edu education "
"ee eg email enterprises equipment er es estate et eu events expert exposed farm "
"fi fish fj fk flights florist fm fo foundation fr futbol ga gallery gb gd ge gf "
"gg gh gi gift gl glass gm gn gov gp gq gr graphics gs gt gu guitars guru gw gy "
"hk hm hn holdings holiday house hr ht hu id ie il im immobilien in industries "
"info institute int international io iq ir is it je jm jo jobs jp kaufen ke kg "
"kh ki kim kitchen kiwi km kn kp kr kred kw ky kz la land lb lc li lighting limo "
"link lk lr ls lt lu luxury lv ly ma maison management mango marketing mc md me "
"menu mg mh mil mk ml mm mn mo mobi moda monash mp mq mr ms mt mu museum mv mw "
"mx my mz na nagoya name nc ne net neustar nf ng ni ninja nl no np nr nu nz om "
"onl org pa partners parts pe pf pg ph photo photography photos pics pink pk pl "
"plumbing pm pn post pr pro productions properties ps pt pub pw py qa qpon re "
"recipes red rentals repair report reviews rich ro rs ru ruhr rw sa sb sc sd se "
"sexy sg sh shiksha shoes si singles sj sk sl sm sn so social solar solutions sr "
"st su supplies supply support sv sx sy systems sz tattoo tc td technology tel "
"tf tg th tienda tips tj tk tl tm tn to today tokyo tools tp tr training travel "
"tt tv tw tz ua ug uk uno us uy uz va vacations vc ve ventures vg vi viajes "
"villas vision vn voting voyage vu wang watch wed wf wien wiki works ws xxx xyz "
"ye yt za zm zone zw ".split())

def normalize(value):
    return re.sub('\s', '', value.lower())

def validate(value):
    "tld must be present in list of actual tld's. 2014-03-02"
    return value in tlds

def _beautify(tlds, length = 80):
    r = sorted(set([t.strip().lower() for t in tlds.split() if not t.lower().startswith('xn--')]))
    b1, b2 = min([len(tld) for tld in r]), max([len(tld) for tld in r])
    result = []
    line = []
    while r:
        while r and sum([len(s) for s in line]) + len(line) + len(r[0]) + 1 <= length:
            line.append(r[0])
            r = r[1:]
        result.append(' '.join(line))
        line = []
    print "pattern = '\w{{{},{}}}'".format(b1, b2)
    print 'tlds = set('
    for l in result:
        print '"{} "'.format(l)
    print '.split()'

if __name__ == '__main__':
    _beautify("""
AC
ACADEMY
ACTOR
AD
AE
AERO
AF
AG
AGENCY
AI
AL
AM
AN
AO
AQ
AR
ARPA
AS
ASIA
AT
AU
AW
AX
AZ
BA
BAR
BARGAINS
BB
BD
BE
BERLIN
BEST
BF
BG
BH
BI
BIKE
BIZ
BJ
BLUE
BM
BN
BO
BOUTIQUE
BR
BS
BT
BUILD
BUILDERS
BUZZ
BV
BW
BY
BZ
CA
CAB
CAMERA
CAMP
CARDS
CAREERS
CAT
CATERING
CC
CD
CENTER
CEO
CF
CG
CH
CHEAP
CHRISTMAS
CI
CK
CL
CLEANING
CLOTHING
CLUB
CM
CN
CO
CODES
COFFEE
COM
COMMUNITY
COMPANY
COMPUTER
CONDOS
CONSTRUCTION
CONTRACTORS
COOL
COOP
CR
CRUISES
CU
CV
CW
CX
CY
CZ
DANCE
DATING
DE
DEMOCRAT
DIAMONDS
DIRECTORY
DJ
DK
DM
DO
DOMAINS
DZ
EC
EDU
EDUCATION
EE
EG
EMAIL
ENTERPRISES
EQUIPMENT
ER
ES
ESTATE
ET
EU
EVENTS
EXPERT
EXPOSED
FARM
FI
FISH
FJ
FK
FLIGHTS
FLORIST
FM
FO
FOUNDATION
FR
FUTBOL
GA
GALLERY
GB
GD
GE
GF
GG
GH
GI
GIFT
GL
GLASS
GM
GN
GOV
GP
GQ
GR
GRAPHICS
GS
GT
GU
GUITARS
GURU
GW
GY
HK
HM
HN
HOLDINGS
HOLIDAY
HOUSE
HR
HT
HU
ID
IE
IL
IM
IMMOBILIEN
IN
INDUSTRIES
INFO
INSTITUTE
INT
INTERNATIONAL
IO
IQ
IR
IS
IT
JE
JM
JO
JOBS
JP
KAUFEN
KE
KG
KH
KI
KIM
KITCHEN
KIWI
KM
KN
KP
KR
KRED
KW
KY
KZ
LA
LAND
LB
LC
LI
LIGHTING
LIMO
LINK
LK
LR
LS
LT
LU
LUXURY
LV
LY
MA
MAISON
MANAGEMENT
MANGO
MARKETING
MC
MD
ME
MENU
MG
MH
MIL
MK
ML
MM
MN
MO
MOBI
MODA
MONASH
MP
MQ
MR
MS
MT
MU
MUSEUM
MV
MW
MX
MY
MZ
NA
NAGOYA
NAME
NC
NE
NET
NEUSTAR
NF
NG
NI
NINJA
NL
NO
NP
NR
NU
NZ
OM
ONL
ORG
PA
PARTNERS
PARTS
PE
PF
PG
PH
PHOTO
PHOTOGRAPHY
PHOTOS
PICS
PINK
PK
PL
PLUMBING
PM
PN
POST
PR
PRO
PRODUCTIONS
PROPERTIES
PS
PT
PUB
PW
PY
QA
QPON
RE
RECIPES
RED
RENTALS
REPAIR
REPORT
REVIEWS
RICH
RO
RS
RU
RUHR
RW
SA
SB
SC
SD
SE
SEXY
SG
SH
SHIKSHA
SHOES
SI
SINGLES
SJ
SK
SL
SM
SN
SO
SOCIAL
SOLAR
SOLUTIONS
SR
ST
SU
SUPPLIES
SUPPLY
SUPPORT
SV
SX
SY
SYSTEMS
SZ
TATTOO
TC
TD
TECHNOLOGY
TEL
TF
TG
TH
TIENDA
TIPS
TJ
TK
TL
TM
TN
TO
TODAY
TOKYO
TOOLS
TP
TR
TRAINING
TRAVEL
TT
TV
TW
TZ
UA
UG
UK
UNO
US
UY
UZ
VA
VACATIONS
VC
VE
VENTURES
VG
VI
VIAJES
VILLAS
VISION
VN
VOTING
VOYAGE
VU
WANG
WATCH
WED
WF
WIEN
WIKI
WORKS
WS
XN--3BST00M
XN--3DS443G
XN--3E0B707E
XN--45BRJ9C
XN--55QW42G
XN--55QX5D
XN--6FRZ82G
XN--6QQ986B3XL
XN--80AO21A
XN--80ASEHDB
XN--80ASWG
XN--90A3AC
XN--CG4BKI
XN--CLCHC0EA0B2G2A9GCD
XN--D1ACJ3B
XN--FIQ228C5HS
XN--FIQ64B
XN--FIQS8S
XN--FIQZ9S
XN--FPCRJ9C3D
XN--FZC2C9E2C
XN--GECRJ9C
XN--H2BRJ9C
XN--IO0A7I
XN--J1AMH
XN--J6W193G
XN--KPRW13D
XN--KPRY57D
XN--L1ACC
XN--LGBBAT1AD8J
XN--MGB9AWBF
XN--MGBA3A4F16A
XN--MGBAAM7A8H
XN--MGBAB2BD
XN--MGBAYH7GPA
XN--MGBBH1A71E
XN--MGBC0A9AZCG
XN--MGBERP4A5D4AR
XN--MGBX4CD0AB
XN--NGBC5AZD
XN--O3CW4H
XN--OGBPF8FL
XN--P1AI
XN--PGBS0DH
XN--Q9JYB4C
XN--S9BRJ9C
XN--UNUP4Y
XN--WGBH1C
XN--WGBL6A
XN--XKC2AL3HYE2A
XN--XKC2DL3A5EE0H
XN--YFRO4I67O
XN--YGBI2AMMX
XN--ZFR164B
XXX
XYZ
YE
YT
ZA
ZM
ZONE
ZW""")    