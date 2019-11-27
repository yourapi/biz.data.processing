#from ming import Session
#from ming.datastore import DataStore
#from ming.odm import ThreadLocalODMSession

#bind = DataStore('mongodb://localhost:27017/', database='kpn_swol')
##bind = DataStore('mim://localhost:27017/', database='odm_tutorial')
#doc_session = Session(bind)
#session = ThreadLocalODMSession(doc_session=doc_session)

import indroid.db.micromongo as micromongo
connect = micromongo.connect()
clean_connection = micromongo.clean_connection()