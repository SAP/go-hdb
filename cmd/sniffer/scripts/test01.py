# SPDX-FileCopyrightText: 2014-2021 SAP SE
#
# SPDX-License-Identifier: Apache-2.0

from hdbcli import dbapi

conn = dbapi.connect(
    address="localhost",
    port=50000,
    user="SYSTEM",
    password="Toor1234"
)

cursor = conn.cursor()

cursor.execute("DROP TABLE T1")

cursor.execute("CREATE TABLE T1 (ID INTEGER PRIMARY KEY, C2 VARCHAR(255))")
cursor.close()

sql = 'INSERT INTO T1 (ID, C2) VALUES (?, ?)'
cursor = conn.cursor()
cursor.execute(sql, (1, 'hello'))
# returns True
cursor.execute(sql, (2, 'hello again'))
# returns True
cursor.close()

sql = 'INSERT INTO T1 (ID, C2) VALUES (:id, :c2)'
cursor = conn.cursor()
id = 3
c2 = "goodbye"
cursor.execute(sql, {"id": id, "c2": c2})
# returns True
cursor.close()

sql = 'SELECT * FROM T1'
cursor = conn.cursor()
cursor.execute(sql)
for row in cursor:
    print(row)
