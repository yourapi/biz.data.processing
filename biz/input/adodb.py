"""Very schematic code. To enhance further, of course."""
from __future__ import absolute_import
import os
import win32com.client

conn = win32com.client.Dispatch("ADODB.Connection")

# Either way works: one is the Jet OLEDB driver, the other is the
# Access ODBC driver.  OLEDB is probably better.

db = r"c:\dev\54nsdc\Volunteer.mdb"
DSN="Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + db
#DSN="Driver={Microsoft Access Driver (*.mdb)};DBQ=" + db
conn.Open(DSN)

rs = win32com.client.Dispatch("ADODB.Recordset")
rs.Open( "[Committees]", conn, 1, 3 )

print rs.Fields.Count, " fields found:"
for x in range(rs.Fields.Count):
    print rs.Fields.Item(x).Name,
    print rs.Fields.Item(x).Type,
    print rs.Fields.Item(x).DefinedSize,
    print rs.Fields.Item(x).Value